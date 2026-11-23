[← Documentation Map](README.md) · [Next: 02 · Synchronous EventBus and Routing →](sync-bus-and-routing.md)

# 01 · Getting Started and Architecture

This guide covers installing `flowevent`, why naive Python observer lists fail in production, core architectural boundaries, and working sync/async starter examples.

---

## Table of Contents

- [Installation](#installation)
- [The Problem: Why Naive Callback Lists Break Down](#the-problem-why-naive-callback-lists-break-down)
- [Architectural Decisions ("Why Is It Designed This Way?")](#architectural-decisions-why-is-it-designed-this-way)
  - [Keeping Sync and `asyncio` Strictly Separate](#keeping-sync-and-asyncio-strictly-separate)
  - [In-Process Boundaries](#in-process-boundaries)
  - [Selective C Core with Pure-Python Parity](#selective-c-core-with-pure-python-parity)
- [First Synchronous Example and `PublishResult`](#first-synchronous-example-and-publishresult)
- [First Async Example](#first-async-example)

---

## Installation

`flowevent` requires **Python 3.10 or newer** with zero runtime dependencies. Prebuilt wheels for major OS/CPU combinations bundle the optional compiled `flowevent._core` extension automatically; environments without a C compiler fall back seamlessly to pure Python.

```bash
# Install from PyPI
python -m pip install flowevent

# Editable install from the repository root
python -m pip install -e .

# Include development and benchmark tools
python -m pip install -e ".[dev]"
```

[Back to top](#table-of-contents)

---

## The Problem: Why Naive Callback Lists Break Down

In-process pub/sub almost always starts as a tiny loop:

```python
listeners = []


def publish(event):
    for fn in listeners:
        fn(event)
```

Under concurrent or real application workloads, five subtle bugs quickly appear:

1. **Mutation during iteration:** Unsubscribing inside a callback (`once`) raises `RuntimeError: list changed during iteration` or skips adjacent subscribers.
2. **Concurrent `once` race conditions:** Two threads calling `publish()` concurrently can invoke a "once-only" callback twice.
3. **One exception breaks everyone:** A single failing subscriber aborts delivery to remaining healthy subscribers or obscures who failed.
4. **History replay gaps (`replay` races):** Subscribing with "send last 10 events, then live events" easily drops or duplicates events arriving between the history read and live registration.
5. **Unbounded task creation across thread boundaries:** Calling `loop.call_soon_threadsafe(asyncio.create_task, ...)` on every publish floods memory when an async consumer falls behind.

`flowevent` eliminates these failure modes explicitly by construction.

[Back to top](#table-of-contents)

---

## Architectural Decisions ("Why Is It Designed This Way?")

### Keeping Sync and `asyncio` Strictly Separate

`EventBus` is purely synchronous—it never spawns a hidden background `asyncio` event loop or daemon dispatch loop. Conversely, `AsyncEventBus` is natively built for `asyncio` and runs directly on your owning loop rather than wrapping blocking queues in worker threads.

- Synchronous apps (CLI tools, Flask/Django, worker processes) pay zero async overhead.
- Cross-world communication happens only through explicit bridges (`AsyncEventQueue` or `AsyncEventBus.publish_threadsafe`).

### In-Process Boundaries

- Payloads stay ordinary Python objects in memory—no serialization tax.
- When you need cross-process durability, crash recovery, or network delivery across services, use Kafka, RabbitMQ, Redis Streams, or NATS instead.

### Selective C Core with Pure-Python Parity

Every contract and concurrency guarantee is implemented cleanly in pure Python first. Only the hot synchronous `EventBus` dispatch path adds an optional C extension (`flowevent._core`), keeping the codebase maintainable while removing interpreter overhead where it actually matters.

[Back to top](#table-of-contents)

---

## First Synchronous Example and `PublishResult`

`publish()` returns an immutable **`PublishResult`** summary rather than `None`, and `bus.stats` tracks cumulative `EventBusStats`:

```python
from dataclasses import dataclass
from flowevent import EventBus


@dataclass(frozen=True)
class OrderPlaced:
    order_id: str


seen: list[str] = []

with EventBus[OrderPlaced]() as bus:
    with bus.subscribe(lambda e: seen.append(e.order_id)):
        result = bus.publish(OrderPlaced("ord-101"))

assert seen == ["ord-101"]
assert result.matched == 1
assert result.delivered == 1
assert result.failed == 0
assert result.successful is True
assert bus.stats.published == 1
```

[Back to top](#table-of-contents)

---

## First Async Example

```python
import asyncio
from flowevent import AsyncEventBus


async def main() -> None:
    received: list[str] = []

    async def on_event(msg: str) -> None:
        await asyncio.sleep(0)
        received.append(msg)

    async with AsyncEventBus[str]() as bus:
        sub = await bus.subscribe(on_event)
        async with sub:
            result = await bus.publish("ready")

    assert result.delivered == 1
    assert received == ["ready"]


asyncio.run(main())
```

[Back to top](#table-of-contents)

---

**Previous:** [Documentation Map](README.md) · **Next:** [02 · Synchronous EventBus and Routing →](sync-bus-and-routing.md)
