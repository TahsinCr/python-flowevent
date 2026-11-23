[![CI status][ci-shield]][ci]
[![PyPI version][pypi-shield]][pypi]
[![Python 3.10+][python-shield]][python]
[![MIT License][license-shield]][license]
<br>
**English** | [Türkçe][readme-tr]

# FlowEvent

[Repository][repository] · [Changelog][changelog] · [Latest release][releases] · [MIT License][license]

In-process event delivery for Python applications and libraries. Use a synchronous or async bus to publish typed events and manage subscriptions without a message broker.

> [English documentation](docs/en/README.md) covers architecture choices, indexed routing, bridges, replay buffering, the C extension, and the API reference. Also available in [Türkçe](docs/tr/README.md).

FlowEvent began as the event subsystem of [Vehicle Autonomy Core][vehicle-autonomy-core].

## Contents

- [Getting started](#getting-started)
- [Core concepts](#core-concepts)
- [Working with events](#working-with-events)
- [Async applications](#async-applications)
- [Performance and the C extension](#performance)
- [Development and license](#development)

<a id="getting-started"></a>

## Getting started

### Install

Install from the project root with Python 3.10 or later.

~~~sh
python -m pip install flowevent
~~~

### Publish your first event

A publisher hands an ordinary Python object to the bus. The subscriber receives it before the synchronous publish returns. Leaving the inner context cancels the subscription.

~~~python
from dataclasses import dataclass

from flowevent import EventBus


@dataclass(frozen=True)
class OrderPlaced:
    order_id: str


def send_confirmation(event: OrderPlaced) -> None:
    print(f"Order placed: {event.order_id}")


with EventBus[OrderPlaced]() as bus:
    with bus.subscribe(send_confirmation):
        result = bus.publish(OrderPlaced("order-42"))
        assert result.delivered == 1
~~~

<a id="core-concepts"></a>

## Core concepts

Choose `EventBus` for synchronous handlers and `AsyncEventBus` for coroutines. Use named channels when event groups have separate ownership. `AsyncEventQueue` connects a synchronous publisher to an async consumer. Delivery stays inside one process. Type hints describe the expected event to your editor and type checker. Apply a filter when you need to select events at runtime.

<a id="working-with-events"></a>

## Working with events

### Filter and replay recent events

Give the bus a bounded history, then subscribe with a routing key. The example replays the latest matching event and receives subsequent live matches using the same subscription.

~~~python
from flowevent import EventBus

received = []
with EventBus[tuple[str, str]](history=3, routing_key=lambda event: event[0]) as bus:
    bus.publish(("orders", "created"))
    bus.publish(("billing", "invoice_sent"))
    with bus.subscribe(received.append, routing_key="orders", replay=1):
        bus.publish(("orders", "packed"))

assert received == [("orders", "created"), ("orders", "packed")]
~~~

<a id="async-applications"></a>

## Async applications

### Publish to coroutine handlers

Use `AsyncEventBus` when handlers already perform async work. Keep the bus and its subscriptions on the event loop that owns them.

If you opt into `DeliveryMode.CONCURRENT`, set `max_concurrency` to bound the number of handlers run concurrently by each publish. Without that limit, a publish can create a task for every matching handler.

~~~python
import asyncio

from flowevent import AsyncEventBus


async def main() -> None:
    received = []

    async def collect(event: str) -> None:
        received.append(event)

    async with AsyncEventBus[str]() as bus:
        subscription = await bus.subscribe(collect)
        async with subscription:
            await bus.publish("order.shipped")

    assert received == ["order.shipped"]


asyncio.run(main())
~~~

<a id="performance"></a>

## Performance and the C extension

The optional C core reduces dispatch overhead for supported synchronous `EventBus` paths. This matters most when publishing frequently or routing to many subscribers with small handlers. The pure-Python implementation provides the same API. See the [benchmark archive][benchmarks] for workloads, environments, and measurements from published releases.

<a id="development"></a>

## Development and license

~~~sh
python -m pip install -e ".[dev]"
python run_tests.py
~~~

Run `python run_tests.py --native` to rebuild and require the C extension for the tests.

This project is licensed under the [MIT License][license].

## Related projects

[FlowCoord][repo-flowcoord] · [FlowPipeline][repo-flowpipeline]

[ci-shield]: https://img.shields.io/github/actions/workflow/status/TahsinCr/python-flowevent/ci.yml?branch=main&event=push&label=CI&style=for-the-badge&logo=github&logoColor=white
[pypi-shield]: https://img.shields.io/pypi/v/flowevent?label=PyPI&style=for-the-badge&logo=pypi&logoColor=white
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge
[ci]: https://github.com/TahsinCr/python-flowevent/actions/workflows/ci.yml?query=branch%3Amain+event%3Apush
[pypi]: https://pypi.org/project/flowevent/
[python]: https://www.python.org/downloads/
[readme-tr]: https://github.com/TahsinCr/python-flowevent/blob/main/README-TR.md
[repository]: https://github.com/TahsinCr/python-flowevent
[changelog]: https://github.com/TahsinCr/python-flowevent/blob/main/CHANGELOG.md
[license]: https://github.com/TahsinCr/python-flowevent/blob/main/LICENSE
[repo-flowcoord]: https://github.com/TahsinCr/python-flowcoord
[repo-flowpipeline]: https://github.com/TahsinCr/python-flowpipeline
[releases]: https://github.com/TahsinCr/python-flowevent/releases/latest
[benchmarks]: https://github.com/TahsinCr/python-flowevent/blob/main/benchmarks/results/README.md
[vehicle-autonomy-core]: https://github.com/TahsinCr/vehicle-autonomy-core
