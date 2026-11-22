"""Benchmark subscription lifecycles and callback invocation."""

from __future__ import annotations

from flowevent.callback import CallbackSubscription
from flowevent.scopes import SubscriptionScope

from ._runner import bench_async, bench_sync, create_runner


def _identity(value: int) -> int:
    return value


async def _async_identity(value: int) -> int:
    return value


def main() -> None:
    runner = create_runner(__doc__ or "FlowEvent lifecycle benchmarks")
    runner.parse_args()

    callback = CallbackSubscription(1, lambda: None, _identity)
    bench_sync(runner, "callback/plain-invoke", lambda: callback.invoke(1))

    hooked_callback = CallbackSubscription(
        2,
        lambda: None,
        _identity,
        predicate=lambda event: event > 0,
        on_before=lambda _context: None,
        on_success=lambda _context: None,
        on_after=lambda _context: None,
    )
    bench_sync(runner, "callback/predicate-hooks-invoke", lambda: hooked_callback.invoke(1))

    async_callback = CallbackSubscription(3, lambda: None, _async_identity, asynchronous=True)
    bench_async(runner, "callback/async-invoke", lambda: async_callback.invoke_async(1))

    def subscription_scope() -> int:
        with SubscriptionScope() as scope:
            scope.add(CallbackSubscription(4, lambda: None, _identity))
            return 1

    bench_sync(runner, "subscription-scope/add-close", subscription_scope)


if __name__ == "__main__":
    main()
