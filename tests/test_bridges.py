"""Thread-to-async bridge behavior and ownership tests."""

from __future__ import annotations

import asyncio
import threading
import unittest
from unittest.mock import patch

from flowevent import ErrorPolicy, EventBus
from flowevent.bridges import AsyncEventQueue
from flowevent.errors import EventQueueClosedError


class EventQueueBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_event_queue_wakes_waiter_when_observer_raises(self):
        bus = EventBus[int](error_policy=ErrorPolicy.RAISE)

        class Observer:
            def record(self, name, **fields):
                if name == "event_queue.publish":
                    raise RuntimeError("observer")

        observer = Observer()
        queue = AsyncEventQueue(bus, observer=observer)
        consumer = asyncio.create_task(queue.receive())
        await asyncio.sleep(0)

        try:
            with self.assertLogs("flowevent.bridges", level="ERROR"):
                self.assertTrue(bus.publish(7).successful)
            self.assertEqual(await asyncio.wait_for(consumer, timeout=1), 7)
        finally:
            await queue.aclose()
            bus.close()

    async def test_event_queue_uses_local_wakeup_for_same_thread_publish(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        consumer = asyncio.create_task(queue.receive())
        await asyncio.sleep(0)
        loop = asyncio.get_running_loop()

        try:
            with patch.object(
                loop,
                "call_soon_threadsafe",
                side_effect=AssertionError("same-thread wake used cross-thread scheduling"),
            ):
                bus.publish(7)
            self.assertEqual(await asyncio.wait_for(consumer, timeout=1), 7)
        finally:
            await queue.aclose()
            bus.close()

    async def test_event_queue_wakes_waiter_from_publisher_thread(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        consumer = asyncio.create_task(queue.receive())
        await asyncio.sleep(0)
        errors: list[BaseException] = []
        loop = asyncio.get_running_loop()

        def publish() -> None:
            try:
                bus.publish(11)
            except BaseException as error:
                errors.append(error)

        publisher = threading.Thread(target=publish, name="EventQueuePublisherTest")
        try:
            with patch.object(
                loop,
                "call_soon_threadsafe",
                wraps=loop.call_soon_threadsafe,
            ) as schedule_threadsafe:
                publisher.start()
                self.assertEqual(await asyncio.wait_for(consumer, timeout=1), 11)
            publisher.join(timeout=1)
            self.assertFalse(publisher.is_alive())
            self.assertEqual(errors, [])
            schedule_threadsafe.assert_called_once()
        finally:
            await queue.aclose()
            bus.close()

    async def test_event_queue_recovers_when_publish_races_with_waiter_cancellation(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        consumer = asyncio.create_task(queue.receive())
        await asyncio.sleep(0)
        self.assertIsNotNone(queue._waiter)

        publisher = threading.Thread(target=lambda: bus.publish(13))
        try:
            publisher.start()
            publisher.join(timeout=1)
            self.assertFalse(publisher.is_alive())
            consumer.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await consumer
            self.assertEqual(await asyncio.wait_for(queue.receive(), timeout=1), 13)
        finally:
            await queue.aclose()
            bus.close()

    async def test_event_queue_rejects_second_consumer_and_other_loop(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        bus.publish(1)
        self.assertEqual(await queue.receive(), 1)

        async def receive_again():
            return await queue.receive()

        second = asyncio.create_task(receive_again())
        with self.assertRaises(RuntimeError):
            await second

        errors = []

        def use_from_another_loop():
            try:
                asyncio.run(queue.receive())
            except RuntimeError as error:
                errors.append(error)

        thread = threading.Thread(target=use_from_another_loop)
        thread.start()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        await queue.aclose()
        with self.assertRaises(EventQueueClosedError):
            await queue.receive()
        bus.publish(2)
        self.assertEqual(queue.stats().received, 1)
        bus.close()

    async def test_event_queue_allows_consumer_takeover_after_task_finishes(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)

        async def receive_one():
            return await queue.receive()

        bus.publish(1)
        first = asyncio.create_task(receive_one())
        self.assertEqual(await first, 1)

        bus.publish(2)
        second = asyncio.create_task(receive_one())
        self.assertEqual(await second, 2)
        await queue.aclose()
        bus.close()

    async def test_event_queue_can_unsubscribe_after_owning_loop_closes(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        errors = []

        def bind_and_close_loop():
            loop = asyncio.new_event_loop()

            async def attach_cancelled_consumer():
                consumer = asyncio.create_task(queue.receive())
                await asyncio.sleep(0)
                consumer.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await consumer

            try:
                loop.run_until_complete(attach_cancelled_consumer())
            except BaseException as error:
                errors.append(error)
            finally:
                loop.close()

        thread = threading.Thread(target=bind_and_close_loop)
        thread.start()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])

        await queue.aclose()
        self.assertTrue(queue.closed)
        self.assertEqual(bus.subscriber_count, 0)
        bus.close()

    async def test_event_queue_recovers_a_cancelled_waiter_in_the_same_consumer(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)

        async def recover():
            try:
                await queue.receive()
            except asyncio.CancelledError:
                return await queue.receive()

        consumer = asyncio.create_task(recover())
        await asyncio.sleep(0)
        consumer.cancel()
        await asyncio.sleep(0)
        bus.publish(9)
        self.assertEqual(await consumer, 9)
        await queue.aclose()
        bus.close()

    async def test_event_queue_async_context(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        async with queue:
            bus.publish(7)
            self.assertEqual(await queue.receive(), 7)
        bus.publish(8)
        self.assertEqual(queue.stats().received, 1)
        bus.close()
