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

- **Why `block_size`?** A full deque snapshot may copy up to `O(N)` items after history changes. Sealed blocks can be shared, reducing item copies for filtered replay. The default deque is a good fit for append-heavy workloads.
- `bus.latest(...)` returns the newest match. `bus.query(..., limit=N)` returns up to the latest `N` matches in chronological order. Depending on the filter and storage, it can still scan or snapshot history.

[Back to top](#table-of-contents)

---

## Replay Buffering: Replaying Past Events Without Losing Live Ones

When calling `bus.subscribe(cb, replay=5)`, `flowevent` prevents both missed and out-of-order events:

1. Registers the subscriber in a `replaying` state and captures the history snapshot.
2. Live publishes occurring while historical items are being replayed append into the subscriber's private replay buffer (bounded by `replay_buffer_limit`, default `1_000`).
3. Buffered events are delivered before direct delivery resumes. If the buffer fills, the overflowing publish reports a `BufferError` through its error policy and the replaying subscription is cancelled.

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

Use `CallbackSubscription.invoke` or `await CallbackSubscription.invoke_async` to apply rate limits, timeouts and hooks. Calling the wrapper directly invokes the original callback without those policies. A synchronous timeout is reported after the callback returns. Async cancellation requires cooperation from the callback.

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
