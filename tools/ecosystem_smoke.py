"""Exercise the ecosystem with independently installed distributions."""

import asyncio

from flowcoord import AsyncTaskScope, OperationStats
from flowpipeline import AsyncPipeline

from flowevent import AsyncEventQueue, EventBus, correlation_context, current_correlation_id


async def main() -> None:
    stats = OperationStats()
    correlations: list[str | None] = []
    pipeline = AsyncPipeline[int](
        observer=lambda observation: correlations.append(observation.correlation_id),
        correlation_provider=current_correlation_id,
    )

    @pipeline.stage("double")
    async def double(value: int) -> int:
        return value * 2

    with correlation_context("ecosystem-smoke"), EventBus[int]() as bus:
        async with AsyncEventQueue(bus, observer=stats) as queue, AsyncTaskScope() as scope:
            assert bus.publish(21).successful
            event = await queue.receive()
            task = scope.start(lambda: pipeline.run(event))
            assert await task.join() == 42

    assert correlations == ["ecosystem-smoke", "ecosystem-smoke"]
    operations = stats.snapshot().operations
    assert operations["event_queue.publish"].calls == operations["event_queue.receive"].calls == 1


if __name__ == "__main__":
    asyncio.run(main())
