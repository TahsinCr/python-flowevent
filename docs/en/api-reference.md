[← Documentation Map](README.md) · [Previous: 05 · Performance and C Core](performance-and-c-core.md)

# 06 · FlowEvent API Reference

Complete reference for every public class, constructor, method, dataclass, enum, and exception exported by `flowevent` (`v2.0.0`).

---

## Table of Contents

- [`EventBus` (Synchronous)](#eventbus-synchronous)
- [`AsyncEventBus` (Asyncio)](#asynceventbus-asyncio)
- [`EventEngine` and `AsyncEventEngine`](#eventengine-and-asynceventengine)
- [`AsyncEventQueue` and Observer](#asynceventqueue-and-observer)
- [History, Filtering, Hook, and Context Types](#history-filtering-hook-and-context-types)
- [Subscription, Scope, and Callback Utilities](#subscription-scope-and-callback-utilities)
- [Exception Hierarchy](#exception-hierarchy)

---

## `EventBus` (Synchronous)

Subclasses `BaseEventBus[T]`.

### Constructor

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `history` | `EventHistory[T] \| int \| None` | `None` | Passing an `int` creates a `MemoryEventHistory(history)`. |
| `routing_key` | `Callable[[T], Hashable] \| None` | `None` | Key extractor enabling indexed subscriber routing. |
| `error_policy` | `ErrorPolicy` | `ErrorPolicy.ISOLATE` | `"isolate"` or `"raise"`. |
| `executor` | `ThreadPoolExecutor \| None` | `None` | Optional thread pool for handler dispatch. |
| `replay_buffer_limit` | `int` | `1000` | Max live events buffered per subscriber during replay. |
| `max_schedules` | `int` | `64` | Maximum concurrent `publish_every` schedules. |
| `shutdown_timeout` | `float` | `5.0` | Timeout budget (seconds) for stopping schedules in `close()`. |
| `actions` / `on_*` | `EventBusActions[T] \| Callable` | `None` | `on_before`, `on_after`, `on_error`, `on_timeout` (supply one or the other). |

### Key Members

- `publish(event: T) -> PublishResult`
- `subscribe(callback, *, event_filter=None, event_type=None, predicate=None, routing_key=None, once=False, times=None, replay=0) -> Subscription`
- `once(callback, *, event_filter=None, event_type=None, predicate=None, routing_key=None, replay=0) -> Subscription`
- `wait_for(*, event_filter=None, event_type=None, predicate=None, routing_key=None, timeout=None) -> T | None`
- `publish_every(event: T, interval: float, *, times: int | None = None, immediately: bool = True) -> Subscription`
- `latest(...) -> T | None`, `query(..., limit=None) -> tuple[T, ...]`
- `clear() -> None`, `close() -> None` (`with` support), `closed`, `subscriber_count`, `stats -> EventBusStats`, `history`, `error_policy`

[Back to top](#table-of-contents)

---

## `AsyncEventBus` (Asyncio)

Subclasses `BaseEventBus[T]`.

### Constructor

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `history` | `EventHistory[T] \| int \| None` | `None` | Optional event history store. |
| `error_policy` | `ErrorPolicy` | `ErrorPolicy.ISOLATE` | Error reporting mode. |
| `delivery_mode` | `DeliveryMode` | `DeliveryMode.SEQUENTIAL` | `"sequential"` or `"concurrent"`. |
| `max_concurrency` | `int \| None` | `None` | Max simultaneous tasks per publish in `CONCURRENT` mode. |
| `replay_buffer_limit` | `int` | `1000` | Max live events buffered during replay. |
| `replay_scan_batch_size` | `int \| None` | `None` | Periodic yield interval during large history scans. |
| `max_schedules` | `int` | `64` | Maximum active `publish_every` jobs. |
| `actions` / `on_*` | `AsyncEventBusActions[T] \| Callable` | `None` | Coroutine lifecycle hooks. |

### Key Members

- `async publish(event: T) -> PublishResult`
- `publish_threadsafe(event: T) -> concurrent.futures.Future[PublishResult]`
- `async subscribe(callback, *, event_filter=None, event_type=None, predicate=None, once=False, times=None, replay=0) -> AsyncSubscription`
- `async once(...) -> AsyncSubscription`
- `async wait_for(..., timeout=None) -> T | None`
- `publish_every(event: T, interval: float, *, times: int | None = None, immediately: bool = True) -> AsyncSubscription`
- `latest(...)`, `query(..., limit=None)`, `clear()`, `async close()` (`async with` support), `delivery_mode`, `stats`, `subscriber_count`, `closed`

[Back to top](#table-of-contents)

---

## `EventEngine` and `AsyncEventEngine`

Named-channel registry (`running`, `channel_names -> tuple[str, ...]`).

- `EventEngine(*, history=None, error_policy=..., executor=None, actions=None, on_before=..., on_after=..., on_error=..., on_timeout=...)`
- `AsyncEventEngine(*, history=None, error_policy=..., delivery_mode=..., max_concurrency=None, actions=None, on_before=..., on_after=..., on_error=..., on_timeout=...)`
- Methods: `channel(name)`, `add(name, bus)`, `remove(name) -> bool`, `subscribe(channel, cb, ...)`, `once(channel, cb, ...)`, `publish(channel, event)`, `wait_for(channel, ...)`, `publish_every(channel, ...)`, `start()`, `stop()` / `close()` (`remove`, `subscribe`, `once`, `publish`, `wait_for`, `stop`, `close` are coroutines on `AsyncEventEngine`).

[Back to top](#table-of-contents)

---

## `AsyncEventQueue` and Observer

- `AsyncEventQueue(bus: EventBus[T], *, event_filter=None, event_type=None, predicate=None, routing_key=None, capacity=128, overflow='error', observer=None)`
- Members: `async receive() -> T`, `async aclose() -> None`, `stats() -> AsyncEventQueueStats`, `closed -> bool`, `async for` and `async with` support.
- `AsyncEventQueueStats`: `size`, `capacity`, `received`, `dropped`, `overflow_errors`, `closed`.
- `EventQueueObserver`: `record(name, *, status='success', dropped=0, rejected=0, queue_depth=None) -> None`.

[Back to top](#table-of-contents)

---

## History, Filtering, Hook, and Context Types

| Type | Summary |
| --- | --- |
| `MemoryEventHistory(capacity=1_000, *, block_size=None)` | `EventHistory[T]` implementation (`append`, `latest`, `query`, `clear`, `__len__`, `capacity`). |
| `EventFilter(event_type=None, predicate=None)` | Type (`isinstance`) and synchronous `bool` predicate filter (`matches(event)`). |
| `DeliveryMode` / `ErrorPolicy` | `StrEnum`: `SEQUENTIAL` / `CONCURRENT`, `ISOLATE` / `RAISE`. |
| `PublishResult` | `matched`, `delivered`, `failed`, `errors: tuple[Exception, ...]`, `successful -> bool`. |
| `EventBusStats` | `published`, `delivered`, `failed`. |
| `EventBusActions` / `AsyncEventBusActions` | `on_before`, `on_after`, `on_error`, `on_timeout`; contexts: `EventErrorContext(event, error)`, `EventTimeoutContext(event_filter, timeout)`. |
| `EventEnvelope(event, id=..., correlation_id=None, source=None, timestamp=..., metadata=...)` | Shallow read-only metadata (`_ReadOnlyMetadata`), UTC timestamp, auto correlation ID. |
| `correlation_context(correlation_id)` / `current_correlation_id()` | Task- and thread-local `ContextVar` helpers. |

[Back to top](#table-of-contents)

---

## Subscription, Scope, and Callback Utilities

| Type | Summary |
| --- | --- |
| `Subscription` / `AsyncSubscription` | `id`, `active`, `cancel()` (`async def` on async variant), `with` / `async with` support. |
| `SubscriptionScope` / `AsyncSubscriptionScope` | `add(handle)`, `close()` / `async aclose()`, `closed`; retains failed handles for retry and raises `ExceptionGroup`. |
| `CancelHandle` / `AsyncCancelHandle` | Cancellation handle protocols. |
| `CallbackSubscription(identifier, cancel, callback, *, asynchronous=False, once=False, max_calls=None, frequency_hz=None, timeout=None, predicate=None, enabled=True, on_before=None, on_success=None, on_error=None, on_timeout=None, on_after=None)` | Managed callback wrapper (`invoke`, `invoke_async`, `enable`, `disable`, `cancel`, hook decorators) + `CallbackContext(event, result=None, error=None, elapsed=0.0)`. |

[Back to top](#table-of-contents)

---

## Exception Hierarchy

```text
RuntimeError
└── EventBusError
    ├── EventBusClosedError
    ├── InvalidEventHandlerError      (also TypeError)
    ├── CallbackTimeoutError          (also TimeoutError)
    ├── EventShutdownTimeoutError     (also TimeoutError)
    ├── EventQueueClosedError
    └── QueueOverflowError
```

[Back to top](#table-of-contents)

---

**Previous:** [05 · Performance and C Core](performance-and-c-core.md) · **Map:** [Documentation Map](README.md)
