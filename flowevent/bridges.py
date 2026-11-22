"""Thread-to-async bridges with explicit loop and shutdown ownership."""

from __future__ import annotations

import asyncio
import threading
from collections import deque
from collections.abc import AsyncIterator, Callable, Hashable
from dataclasses import dataclass
from typing import Any, Generic, Literal, Protocol, TypeVar

from ._callbacks import is_async_callable, synchronous_result
from ._lifecycle import bind_running_loop
from .base import validate_positive_capacity
from .errors import EventQueueClosedError, QueueOverflowError
from .event_bus import EventBus
from .filtering import EventFilter, EventType
from .subscription import Subscription

T = TypeVar("T")
__all__ = ["AsyncEventQueue", "AsyncEventQueueStats", "EventQueueObserver"]


class EventQueueObserver(Protocol):
    """Synchronous operation recorder for queue activity, called outside its lock."""

    def record(
        self,
        name: str,
        *,
        status: str = "success",
        dropped: int = 0,
        rejected: int = 0,
        queue_depth: int | None = None,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class AsyncEventQueueStats:
    size: int
    capacity: int
    received: int
    dropped: int
    overflow_errors: int
    closed: bool


class AsyncEventQueue(Generic[T]):
    """Bounded cross-thread queue subscribed to a synchronous EventBus.

    One lock-protected deque covers every accepted event, including events
    published before the first loop consumer is attached. A publish never
    creates an asyncio task.
    """

    def __init__(
        self,
        bus: EventBus[T],
        *,
        event_filter: EventFilter[T] | None = None,
        event_type: EventType | None = None,
        predicate: Callable[[T], bool] | None = None,
        routing_key: Hashable | None = None,
        capacity: int = 128,
        overflow: Literal["drop_oldest", "drop_newest", "error"] = "error",
        observer: EventQueueObserver | None = None,
    ) -> None:
        if not isinstance(bus, EventBus):
            raise TypeError("AsyncEventQueue requires a synchronous EventBus")
        if observer is not None:
            record = getattr(observer, "record", None)
            if not callable(record) or is_async_callable(record):
                raise TypeError("observer must provide a synchronous record method")
        self._capacity = validate_positive_capacity(capacity, name="Event queue capacity")
        if overflow not in ("drop_oldest", "drop_newest", "error"):
            raise ValueError("overflow must be 'drop_oldest', 'drop_newest', or 'error'")
        self._overflow = overflow
        self._observer = observer
        self._events: deque[T] = deque()
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread_id: int | None = None
        self._waiter: asyncio.Future[None] | None = None
        self._consumer: asyncio.Task[Any] | None = None
        self._received = 0
        self._dropped = 0
        self._overflow_errors = 0
        self._closed = False
        self._subscription: Subscription | None = bus.subscribe(
            self._on_event,
            event_filter=event_filter,
            event_type=event_type,
            predicate=predicate,
            routing_key=routing_key,
        )

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = bind_running_loop(self._loop, "AsyncEventQueue")
        if self._loop_thread_id is None:
            self._loop_thread_id = threading.get_ident()
        self._loop = loop
        return loop

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def _on_event(self, event: T) -> None:
        waiter: asyncio.Future[None] | None
        dropped = 0
        overflow_error = False
        accepted = True
        with self._lock:
            if self._closed:
                return
            if len(self._events) < self._capacity:
                self._events.append(event)
            elif self._overflow == "drop_newest":
                self._dropped += 1
                dropped = 1
                accepted = False
            elif self._overflow == "drop_oldest":
                self._events.popleft()
                self._dropped += 1
                dropped = 1
                self._events.append(event)
            else:
                self._overflow_errors += 1
                overflow_error = True
                accepted = False
            waiter, self._waiter = self._waiter, None
            loop = self._loop
            loop_thread_id = self._loop_thread_id
            depth = len(self._events)
        try:
            if self._observer is not None:
                synchronous_result(
                    self._observer.record(
                        "event_queue.publish",
                        status="error" if overflow_error else "success",
                        dropped=dropped,
                        rejected=int(overflow_error),
                        queue_depth=depth,
                    )
                )
        finally:
            if accepted and waiter is not None and loop is not None:
                try:
                    if threading.get_ident() == loop_thread_id:
                        self._wake(waiter)
                    else:
                        loop.call_soon_threadsafe(self._wake, waiter)
                except RuntimeError:
                    # A closing loop no longer has a consumer; accepted data stays queued.
                    pass
        if overflow_error:
            raise QueueOverflowError("AsyncEventQueue capacity is full")
        if dropped and self._overflow == "drop_newest":
            return

    @staticmethod
    def _wake(waiter: asyncio.Future[None]) -> None:
        if not waiter.done():
            waiter.set_result(None)

    async def receive(self) -> T:
        """Receive one event; events already accepted are drained after close."""
        loop = self._bind_loop()
        task = asyncio.current_task()
        if self._consumer is None or self._consumer.done():
            self._consumer = task
        elif self._consumer is not task:
            raise RuntimeError("AsyncEventQueue supports one consumer task")
        while True:
            with self._lock:
                if self._events:
                    self._received += 1
                    event = self._events.popleft()
                    depth = len(self._events)
                    break
                if self._closed:
                    raise EventQueueClosedError("AsyncEventQueue is closed and drained")
                if self._waiter is not None:
                    raise RuntimeError("Concurrent receives are not supported")
                waiter = loop.create_future()
                self._waiter = waiter
            try:
                await waiter
            except BaseException:
                with self._lock:
                    if self._waiter is waiter:
                        self._waiter = None
                raise
        if self._observer is not None:
            synchronous_result(self._observer.record("event_queue.receive", queue_depth=depth))
        return event

    def __aiter__(self) -> AsyncIterator[T]:
        self._bind_loop()
        return self

    async def __anext__(self) -> T:
        try:
            return await self.receive()
        except EventQueueClosedError as error:
            raise StopAsyncIteration from error

    def stats(self) -> AsyncEventQueueStats:
        with self._lock:
            return AsyncEventQueueStats(
                len(self._events),
                self._capacity,
                self._received,
                self._dropped,
                self._overflow_errors,
                self._closed,
            )

    async def aclose(self) -> None:
        owner = self._loop
        if owner is not None and owner.is_closed():
            loop = None
        else:
            loop = self._bind_loop()
        with self._lock:
            self._closed = True
            waiter, self._waiter = self._waiter, None
            subscription = self._subscription
        error: BaseException | None = None
        if subscription is not None:
            try:
                subscription.cancel()
            except BaseException as exc:
                error = exc
            else:
                self._subscription = None
        if waiter is not None and loop is not None:
            loop.call_soon(self._wake, waiter)
        if error is not None:
            raise error

    async def __aenter__(self) -> AsyncEventQueue[T]:
        self._bind_loop()
        if self.closed:
            raise RuntimeError("AsyncEventQueue is closed")
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()
