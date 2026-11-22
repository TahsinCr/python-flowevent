"""Synchronous and asyncio-native in-process event delivery."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

__version__ = "1.4.2"

from .actions import (
    AsyncEventBusActions,
    EventBusActions,
    EventErrorContext,
    EventTimeoutContext,
)
from .async_event_bus import AsyncEventBus
from .base import BaseEventBus
from .callback import CallbackContext, CallbackSubscription
from .contracts import DeliveryMode, ErrorPolicy, EventBusStats, PublishResult
from .engine import AsyncEventEngine, EventEngine
from .errors import (
    CallbackTimeoutError,
    EventBusClosedError,
    EventBusError,
    EventQueueClosedError,
    EventShutdownTimeoutError,
    InvalidEventHandlerError,
    QueueOverflowError,
)
from .event_bus import EventBus
from .filtering import EventFilter
from .history import EventHistory, MemoryEventHistory
from .subscription import AsyncSubscription, Subscription

if TYPE_CHECKING:
    from .bridges import AsyncEventQueue, AsyncEventQueueStats, EventQueueObserver
    from .context import EventEnvelope, correlation_context, current_correlation_id
    from .scopes import AsyncCancelHandle, AsyncSubscriptionScope, CancelHandle, SubscriptionScope

_OPTIONAL_EXPORTS = {
    **dict.fromkeys(("AsyncEventQueue", "AsyncEventQueueStats", "EventQueueObserver"), ".bridges"),
    **dict.fromkeys(("EventEnvelope", "correlation_context", "current_correlation_id"), ".context"),
    **dict.fromkeys(
        ("AsyncCancelHandle", "AsyncSubscriptionScope", "CancelHandle", "SubscriptionScope"),
        ".scopes",
    ),
}


def __getattr__(name: str) -> Any:
    module = _OPTIONAL_EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))


__all__ = [
    "AsyncCancelHandle",
    "AsyncEventBus",
    "AsyncEventBusActions",
    "AsyncEventEngine",
    "AsyncEventQueue",
    "AsyncEventQueueStats",
    "AsyncSubscription",
    "AsyncSubscriptionScope",
    "BaseEventBus",
    "CallbackContext",
    "CallbackSubscription",
    "CallbackTimeoutError",
    "CancelHandle",
    "DeliveryMode",
    "ErrorPolicy",
    "EventBus",
    "EventBusActions",
    "EventBusClosedError",
    "EventBusError",
    "EventBusStats",
    "EventEngine",
    "EventEnvelope",
    "EventErrorContext",
    "EventFilter",
    "EventHistory",
    "EventQueueClosedError",
    "EventQueueObserver",
    "EventShutdownTimeoutError",
    "EventTimeoutContext",
    "InvalidEventHandlerError",
    "MemoryEventHistory",
    "PublishResult",
    "QueueOverflowError",
    "Subscription",
    "SubscriptionScope",
    "__version__",
    "correlation_context",
    "current_correlation_id",
]
