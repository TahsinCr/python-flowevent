"""Benchmark event envelopes and correlation context."""

from __future__ import annotations

from flowevent.context import EventEnvelope, correlation_context, current_correlation_id

from ._runner import bench_sync, create_runner


def main() -> None:
    runner = create_runner(__doc__ or "FlowEvent component benchmarks")
    runner.parse_args()

    bench_sync(runner, "event-envelope/create", lambda: EventEnvelope(42))

    def read_correlation_context() -> str | None:
        with correlation_context("benchmark"):
            return current_correlation_id()

    bench_sync(runner, "context/correlation-bind-read-reset", read_correlation_context)


if __name__ == "__main__":
    main()
