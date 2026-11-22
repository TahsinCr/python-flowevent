"""Async execution primitive behavior."""

from __future__ import annotations

import asyncio
import threading
import unittest
from unittest.mock import patch

from flowevent import ErrorPolicy, EventBus
from flowevent._compatibility import ExceptionGroup
from flowevent.bridges import AsyncEventQueue
from flowevent.errors import EventQueueClosedError, QueueOverflowError


class AsyncQueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_event_queue_is_bounded_thread_safe_and_drains_on_close(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus, capacity=2, overflow="drop_oldest")
        thread = threading.Thread(target=lambda: [bus.publish(value) for value in (1, 2, 3)])
        thread.start()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(queue.stats().dropped, 1)
        await queue.aclose()
        self.assertEqual([item async for item in queue], [2, 3])
        self.assertTrue(queue.closed)
        bus.close()

    async def test_async_event_queue_error_policy_and_waiter_wakeup(self):
        bus = EventBus[int](error_policy=ErrorPolicy.RAISE)
        queue = AsyncEventQueue(bus, capacity=1, overflow="error")
        bus.publish(1)
        with self.assertRaises(ExceptionGroup) as caught:
            bus.publish(2)
        self.assertTrue(
            any(isinstance(error, QueueOverflowError) for error in caught.exception.exceptions)
        )
        self.assertEqual(queue.stats().overflow_errors, 1)
        self.assertEqual(await queue.receive(), 1)
        waiting_bus = EventBus[int]()
        waiting_queue = AsyncEventQueue(waiting_bus)
        waiting = asyncio.create_task(waiting_queue.receive())
        await asyncio.sleep(0)
        await waiting_queue.aclose()
        with self.assertRaises(EventQueueClosedError):
            await waiting
        await queue.aclose()
        bus.close()
        waiting_bus.close()

    async def test_async_event_queue_drop_newest_keeps_earlier_event(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus, capacity=1, overflow="drop_newest")
        bus.publish(1)
        bus.publish(2)
        self.assertEqual(queue.stats().dropped, 1)
        self.assertEqual(await queue.receive(), 1)
        await queue.aclose()
        bus.close()

    async def test_async_event_queue_rejects_a_second_consumer_task(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        first = asyncio.create_task(queue.receive())
        await asyncio.sleep(0)

        second = asyncio.create_task(queue.receive())
        with self.assertRaisesRegex(RuntimeError, "one consumer task"):
            await second

        bus.publish(7)
        self.assertEqual(await first, 7)
        await queue.aclose()
        bus.close()

    async def test_async_event_queue_retries_failed_unsubscribe_on_close(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        subscription = queue._subscription
        self.assertIsNotNone(subscription)
        cancel = subscription.cancel
        attempts = 0

        def cancel_once_fails():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("unsubscribe failed")
            cancel()

        with patch.object(subscription, "cancel", side_effect=cancel_once_fails):
            with self.assertRaisesRegex(RuntimeError, "unsubscribe failed"):
                await queue.aclose()
            self.assertEqual(bus.subscriber_count, 1)
            await queue.aclose()

        self.assertEqual(attempts, 2)
        self.assertEqual(bus.subscriber_count, 0)
        bus.close()
