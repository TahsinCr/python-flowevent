[← Documentation Map](README.md) · [Previous: 03 · Async EventBus, Engines, and Bridges](async-bus-engine-and-bridges.md) · [Next: 05 · Performance and C Core →](performance-and-c-core.md)

# 04 · History, Filtering, Scopes, and Context

This guide explains bounded history (`MemoryEventHistory` and blocked snapshots), race-free `replay` buffering, `EventFilter`, bulk cancellation scopes (`SubscriptionScope`), advanced callback wrappers (`CallbackSubscription`), and `ContextVar`-backed `EventEnvelope` propagation.

---

## Table of Contents

- [Event History (`MemoryEventHistory`) and Blocked Storage](#event-history-memoryeventhistory-and-blocked-storage)
- [Replay Buffering: Replaying Past Events Without Losing Live Ones](#replay-buffering-replaying-past-events-without-losing-live-ones)
- [`EventFilter` for Type and Predicate Matching](#eventfilter-for-type-and-predicate-matching)
- [`SubscriptionScope` and `AsyncSubscriptionScope`](#subscriptionscope-and-asyncsubscriptionscope)
- [`CallbackSubscription` for Throttling, Timeouts, and Hooks](#callbacksubscription-for-throttling-timeouts-and-hooks)
- [`EventEnvelope` and `correlation_context`](#eventenvelope-and-correlation_context)

---

## Event History (`MemoryEventHistory`) and Blocked Storage

Pass `history=100`, a configured `MemoryEventHistory(capacity=100_000, block_size=256)`, or your own thread-safe `EventHistory` implementation to either bus:

- **Why `block_size`?** Plain ring buffers (`deque(maxlen=capacity)`) must copy up to `O(N)` items on every replay snapshot. With `block_size` set, completed blocks are stored as immutable tuples (`_Blocks`) so snapshots only copy the mutable head and tail edges.
- `bus.latest(...)` fetches the newest match cleanly, while `bus.query(..., limit=N)` scans backward for bounded limits without copying the entire history deque, returning results in chronological (`oldest -> newest`) order.

[Back to top](#table-of-contents)

---

## Replay Buffering: Replaying Past Events Without Losing Live Ones

When calling `bus.subscribe(cb, replay=5)`, `flowevent` prevents both missed and out-of-order events:

1. Registers the subscriber in a `replaying` state and captures the history snapshot.
2. Live publishes occurring while historical items are being replayed append into the subscriber's private replay buffer (bounded by `replay_buffer_limit`, default `1_000`).
3. Once historical replay completes, buffered live events flush in order before transitioning seamlessly to direct live delivery. Exceeding `replay_buffer_limit` raises `BufferError` and cleans up the partial subscription without leaking.

Note: On `AsyncEventBus`, set `replay_scan_batch_size` to yield control (`await asyncio.sleep(0)`) periodically when scanning huge histories so other tasks are not starved.

[Back to top](#table-of-contents)

---

## `EventFilter` for Type and Predicate Matching

Every subscription, `latest`, `query`, and `wait_for` accepts filters either inline (`event_type=...`, `predicate=...`) or as an explicit `EventFilter(...)` instance (never both):

- **Architectural rule:** `predicate` must be **synchronous** and return an exact `bool` (`True`/`False`). Async predicates or truthy non-bools raise `TypeError` (and any returned coroutine is closed cleanly to avoid warnings). Filters execute inside dispatch/query steps and must remain fast and deterministic.

[Back to top](#table-of-contents)

---

## `SubscriptionScope` and `AsyncSubscriptionScope`

Manage arbitrary numbers of subscriptions or any handle providing `cancel()` under one lifecycle scope:

```python
from flowevent import EventBus, SubscriptionScope

seen: list[int] = []
with EventBus[int]() as bus:
    with SubscriptionScope() as scope:
        scope.add(bus.subscribe(seen.append))
        scope.add(bus.subscribe(lambda x: seen.append(x * 10)))
        bus.publish(2)
    bus.publish(3)  # Scope exit cancelled both subscriptions in reverse order

assert seen == [2, 20]
```

- Cancels handles in **reverse registration order (LIFO)**.
- Every handle's cancellation is attempted even if one fails; failed handles stay owned so retrying `close()` / `aclose()` retries only the remaining failures (`ExceptionGroup`).
- Closed scopes reject new additions with `RuntimeError`.

[Back to top](#table-of-contents)

---

## `CallbackSubscription` for Throttling, Timeouts, and Hooks

Wrap handlers with `CallbackSubscription` when you need per-subscriber rate throttling (`frequency_hz`), timing budgets (`timeout`, triggering `on_timeout`), dynamic `enable()` / `disable()`, or per-subscriber `on_before` / `on_success` / `on_error` / `on_timeout` / `on_after` decorators. (Timeout tracking checks elapsed time budgets cleanly without spawning abandoned threads to forcibly kill uncooperative C/blocking functions.)

[Back to top](#table-of-contents)

---

## `EventEnvelope` and `correlation_context`

Attach tracing metadata without altering payload types using **`EventEnvelope`**:

- Created inside `with correlation_context("trace-42"):`, `EventEnvelope(payload)` automatically inherits `"trace-42"` (or defaults to its own UUID hex `id`).
- `metadata` is shallow-copied into a read-only mapping supporting `copy.deepcopy` and `pickle`.
- `timestamp` is normalized to UTC (`datetime` without `tzinfo` raises `ValueError`).
- `EventBus(..., executor=...)` copies the active `ContextVar` context into worker threads automatically; `asyncio` tasks propagate `ContextVar` natively.

```python
from flowevent import EventEnvelope, correlation_context, current_correlation_id

with correlation_context("trace-42"):
    assert current_correlation_id() == "trace-42"
    envelope = EventEnvelope({"ok": True}, source="api-gateway", metadata={"v": 1})

assert envelope.correlation_id == "trace-42"
assert envelope.metadata["v"] == 1
```

[Back to top](#table-of-contents)

---

**Previous:** [03 · Async EventBus, Engines, and Bridges](async-bus-engine-and-bridges.md) · **Next:** [05 · Performance and C Core →](performance-and-c-core.md)
