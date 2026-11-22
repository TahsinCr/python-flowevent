"""Explicit ownership of existing subscription and cancellation handles."""

from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Awaitable
from typing import Protocol, TypeVar

from ._callbacks import is_async_callable, synchronous_result
from ._lifecycle import (
    bind_running_loop,
    create_task_safely,
    finish_cleanup_errors,
    raise_cleanup_errors,
)

__all__ = [
    "CancelHandle",
    "AsyncCancelHandle",
    "SubscriptionScope",
    "AsyncSubscriptionScope",
]


class CancelHandle(Protocol):
    """A synchronous, preferably idempotent cancellation handle."""

    def cancel(self) -> None: ...


class AsyncCancelHandle(Protocol):
    """A handle whose cancel method returns an awaitable, preferably idempotently."""

    def cancel(self) -> Awaitable[None]: ...


H = TypeVar("H", bound=CancelHandle)
A = TypeVar("A", bound=AsyncCancelHandle)


class SubscriptionScope:
    """Cancel owned handles in reverse registration order on exit.

    All handles are attempted. Failed handles remain owned for a later retry.
    A closed scope rejects additions, including after an unsuccessful close.
    """

    def __init__(self) -> None:
        self._handles: list[CancelHandle] = []
        self._handle_ids: set[int] = set()
        self._lock = threading.RLock()
        self._close_lock = threading.RLock()
        self._closed = False

    @property
    def closed(self) -> bool:
        """Whether the scope has stopped accepting handles."""
        with self._lock:
            return self._closed

    def add(self, handle: H) -> H:
        """Own a handle and return it unchanged; duplicate identities are ignored."""
        cancel = getattr(handle, "cancel", None)
        if not callable(cancel) or is_async_callable(cancel):
            raise TypeError("SubscriptionScope requires a synchronous cancel handle")
        with self._lock:
            if self._closed:
                raise RuntimeError("Cannot add a handle to a closed subscription scope")
            handle_id = id(handle)
            if handle_id not in self._handle_ids:
                self._handles.append(handle)
                self._handle_ids.add(handle_id)
        return handle

    def close(self) -> None:
        """Attempt every cancellation, retaining failures and reporting them together."""
        with self._close_lock:
            with self._lock:
                self._closed = True
                pending, self._handles = list(reversed(self._handles)), []
            failed: list[CancelHandle] = []
            errors: list[BaseException] = []
            for handle in pending:
                try:
                    synchronous_result(handle.cancel())
                except BaseException as error:
                    failed.append(handle)
                    errors.append(error)
            with self._lock:
                self._handles = list(reversed(failed))
                self._handle_ids = {id(handle) for handle in failed}
            raise_cleanup_errors(errors, message="Subscription scope cleanup failed")

    def __enter__(self) -> SubscriptionScope:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class AsyncSubscriptionScope:
    """Own async handles on one loop and finish cleanup despite caller cancellation.

    Cleanup runs sequentially in reverse registration order. Concurrent aclose
    callers await the same cleanup task. Failed handles can be retried.
    """

    def __init__(self) -> None:
        self._handles: list[AsyncCancelHandle] = []
        self._handle_ids: set[int] = set()
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._cleanup: asyncio.Task[list[BaseException]] | None = None

    def _bind_loop(self) -> None:
        self._loop = bind_running_loop(self._loop, "AsyncSubscriptionScope")

    @property
    def closed(self) -> bool:
        """Whether the scope has stopped accepting handles."""
        return self._closed

    def add(self, handle: A) -> A:
        """Own an awaitable cancel handle and return it unchanged.

        The cancel method is not called here because doing so would cancel the
        handle. Its return value is validated when the scope closes.
        """
        if not callable(getattr(handle, "cancel", None)):
            raise TypeError("AsyncSubscriptionScope requires an awaitable cancel handle")
        if self._loop is not None:
            self._bind_loop()
        if self._closed:
            raise RuntimeError("Cannot add a handle to a closed subscription scope")
        handle_id = id(handle)
        if handle_id not in self._handle_ids:
            self._handles.append(handle)
            self._handle_ids.add(handle_id)
        return handle

    async def _cancel_handles(self) -> list[BaseException]:
        pending, self._handles = list(reversed(self._handles)), []
        failed: list[AsyncCancelHandle] = []
        errors: list[BaseException] = []
        for handle in pending:
            try:
                operation = handle.cancel()
                if not inspect.isawaitable(operation):
                    raise TypeError("Async cancel handle cancel() must return an awaitable")
                synchronous_result(await operation)
            except BaseException as error:
                failed.append(handle)
                errors.append(error)
        self._handles = list(reversed(failed))
        self._handle_ids = {id(handle) for handle in failed}
        return errors

    async def aclose(self) -> None:
        """Finish all cancellations; caller cancellation propagates after cleanup."""
        self._bind_loop()
        if asyncio.current_task() is self._cleanup:
            raise RuntimeError("A subscription cleanup cannot wait for its own scope")
        self._closed = True
        cleanup = self._cleanup
        if cleanup is None or (cleanup.done() and self._handles):
            cleanup = create_task_safely(self._cancel_handles(), name="EventFlowScopeCleanup")
            self._cleanup = cleanup
        errors = await finish_cleanup_errors(cleanup, message="Subscription scope cleanup failed")
        raise_cleanup_errors(errors, message="Subscription scope cleanup failed")

    async def __aenter__(self) -> AsyncSubscriptionScope:
        self._bind_loop()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()
