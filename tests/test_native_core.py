"""Tests for loading and running the optional flowevent._core C extension."""

from __future__ import annotations

import gc
import os
import subprocess
import sys
import sysconfig
import textwrap
import unittest
from collections import deque
from collections.abc import Hashable
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from weakref import ref

from flowevent import EventBus, EventFilter, MemoryEventHistory, _native


class NativeRuntimeTests(unittest.TestCase):
    @unittest.skipUnless(_native.CORE is not None, "native core required")
    @unittest.skipIf(sysconfig.get_config_var("Py_GIL_DISABLED"), "heap type refcounts require GIL")
    def test_native_instances_release_owned_heap_type_references(self) -> None:
        core = _native.CORE
        types = (core.Subscriber, core.BusCore)

        def exercise() -> None:
            with EventBus[int]() as bus:
                bus.subscribe(lambda _: None)
                self.assertEqual(bus.publish(1).delivered, 1)

        def references() -> tuple[int, ...]:
            gc.collect()
            return tuple(sys.getrefcount(native_type) for native_type in types)

        exercise()
        before = references()
        for _ in range(64):
            exercise()
        self.assertEqual(references(), before)

    @unittest.skipUnless(_native.CORE is not None, "native core required")
    def test_native_bus_and_routed_subscriber_cycles_release_payloads(self) -> None:
        class Event:
            pass

        def make_cycle():
            bus = EventBus[Event](history=1, routing_key=lambda _: "key")
            event = Event()

            def callback(value: Event) -> None:
                self.assertIs(value, event)
                self.assertEqual(bus.subscriber_count, 1)

            bus.subscribe(callback, routing_key="key")
            self.assertEqual(bus.publish(event).delivered, 1)
            return ref(bus), ref(event)

        if sysconfig.get_config_var("Py_GIL_DISABLED"):
            with ThreadPoolExecutor(4) as pool:
                references = list(pool.map(lambda _: make_cycle(), range(64)))
        else:
            references = [make_cycle()]
        gc.collect()
        for bus_ref, event_ref in references:
            self.assertIsNone(bus_ref())
            self.assertIsNone(event_ref())

    @unittest.skipUnless(sys.version_info >= (3, 14), "requires concurrent.interpreters")
    def test_repeated_isolated_interpreter_shutdown_uses_python_fallback(self) -> None:
        workload = textwrap.dedent("""
            import gc
            from concurrent import interpreters
            from importlib.util import find_spec
            from weakref import ref

            from flowevent import EventBus, _native

            assert interpreters.get_current().id != interpreters.get_main().id
            if find_spec('flowevent._core') is not None:
                try:
                    import flowevent._core
                except ImportError:
                    pass
                else:
                    raise AssertionError('the extension loaded in an isolated interpreter')
            assert _native.CORE is None

            def setup():
                bus = EventBus(history=2, routing_key=lambda event: event % 2)
                received = []
                def receive(event):
                    received.append(bus.subscriber_count)
                bus.subscribe(receive, routing_key=1)
                assert bus.publish(1).delivered == 1
                assert received == [1]
                return bus

            cycle = setup()
            reference = ref(cycle)
            del cycle
            gc.collect()
            assert reference() is None
            cycle_at_shutdown = setup()
        """)
        code = textwrap.dedent("""
            import sys
            from concurrent import interpreters

            for _ in range(5):
                interpreter = interpreters.create()
                try:
                    interpreter.exec(sys.argv[1])
                finally:
                    interpreter.close()
                assert interpreters.list_all() == [interpreters.get_main()]
        """)
        result = subprocess.run(
            [sys.executable, "-c", code, workload], capture_output=True, text=True, timeout=60
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    @unittest.skipUnless(_native.CORE is not None, "native core required")
    def test_native_subscriber_replay_methods_and_replaying_property(self) -> None:
        class InvalidTruthValue:
            def __bool__(self) -> bool:
                raise RuntimeError("truth conversion failed")

        subscriber = _native.CORE.Subscriber(
            lambda _event: None,
            EventFilter(),
            None,
            object(),
            match_all=True,
            pending=deque(maxlen=1),
        )

        self.assertFalse(subscriber.replaying)
        self.assertFalse(subscriber.buffer_if_replaying("ignored"))
        with self.assertRaisesRegex(RuntimeError, "truth conversion failed"):
            subscriber.replaying = InvalidTruthValue()
        self.assertFalse(subscriber.replaying)

        subscriber.replaying = True
        self.assertTrue(subscriber.buffer_if_replaying("queued"))
        with self.assertRaisesRegex(BufferError, "replay buffer is full"):
            subscriber.buffer_if_replaying("overflow")
        self.assertEqual(subscriber.finish_replay(), ("queued",))
        self.assertTrue(subscriber.replaying)
        self.assertIsNone(subscriber.finish_replay())
        self.assertFalse(subscriber.replaying)

    @unittest.skipUnless(_native.CORE is not None, "native core required")
    @unittest.skipIf(sysconfig.get_config_var("Py_GIL_DISABLED"), "allocator hooks require GIL")
    def test_subscriber_recovers_from_allocator_failure(self):
        try:
            import _testcapi
        except ImportError:
            self.skipTest("CPython allocator test hooks unavailable")
        if not hasattr(_testcapi, "set_nomemory"):
            self.skipTest("CPython allocator failure hook unavailable")
        code = textwrap.dedent("""
            import _testcapi
            import gc
            from flowevent import EventBus
            failures = {"claim": 0, "restore_claim": 0}
            for operation in failures:
                for offset in range(40):
                    bus = EventBus()
                    # Stay above CPython 3.15's small-integer cache (through 1024).
                    bus.subscribe(lambda event: None, times=10_000)
                    subscriber = next(iter(bus._subscribers.values()))
                    subscriber.claim()  # initialize native lock before injection
                    before = subscriber.remaining
                    call = getattr(subscriber, operation)
                    failed = False
                    gc.collect()
                    _testcapi.set_nomemory(offset, offset + 1)
                    try:
                        call()
                    except MemoryError:
                        failed = True
                    finally:
                        _testcapi.remove_mem_hooks()
                    if failed:
                        failures[operation] += 1
                        assert subscriber.remaining == before
                    expected = before if failed else before + (1 if operation == "restore_claim" else -1)
                    assert subscriber.remaining == expected
                    subscriber.claim()
                    assert subscriber.remaining == expected - 1
                    bus.close()
            assert all(failures.values()), failures
        """)
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=30
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    @unittest.skipUnless(_native.CORE is not None, "native core required")
    def test_native_condition_cleanup_errors_propagate_and_release_lock(self):
        class Condition:
            def __init__(self, lock, failure):
                self.lock = lock
                self.failure = failure
                self.entered = False

            def __getattribute__(self, name):
                if name == "__exit__" and self.failure == "lookup":
                    raise MemoryError("lookup")
                return object.__getattribute__(self, name)

            def __enter__(self):
                self.lock.acquire()
                self.entered = True

            def notify_all(self):
                if self.failure in ("notify", "both"):
                    raise RuntimeError("notify")

            def __exit__(self, *args):
                self.lock.release()
                if self.failure in ("exit", "both"):
                    raise ValueError("exit")

        for failure, error, message in (
            ("lookup", MemoryError, "lookup"),
            ("notify", RuntimeError, "notify"),
            ("exit", ValueError, "exit"),
            ("both", ValueError, "exit"),
        ):
            with self.subTest(failure=failure), EventBus() as bus:
                bus.subscribe(lambda event: None)
                condition = Condition(bus._lock, failure)
                core = _native.CORE.BusCore(
                    bus._lock,
                    condition,
                    bus._subscribers,
                    bus._unkeyed_subscribers,
                    bus._keyed_subscribers,
                    bus._route_snapshots,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    False,
                    True,
                )
                core.invalidate_snapshots(True, False)
                core.inc_waiters()
                with self.assertRaisesRegex(error, message):
                    core.publish(bus, object())
                self.assertEqual(condition.entered, failure != "lookup")
                # A different thread must acquire it; RLock reentry would hide a leak.
                with ThreadPoolExecutor(max_workers=1) as pool:

                    def acquire_and_release():
                        acquired = bus._lock.acquire(timeout=1)
                        if acquired:
                            bus._lock.release()
                        return acquired

                    self.assertTrue(pool.submit(acquire_and_release).result(timeout=3))
                condition.failure = None
                self.assertEqual(core.publish(bus, object()).delivered, 1)
                core.dec_waiters()

    def test_subscriber_claim_restore_integer_boundaries(self):
        for limit in (1, 2, 256, 1000):
            with self.subTest(limit=limit), EventBus() as bus:
                bus.subscribe(lambda event: None, times=limit)
                subscriber = next(iter(bus._subscribers.values()))
                self.assertEqual(subscriber.claim(), (True, limit == 1))
                self.assertEqual(subscriber.remaining, limit - 1)
                subscriber.restore_claim()
                self.assertEqual(subscriber.remaining, limit)
                for remaining in range(limit - 1, -1, -1):
                    self.assertEqual(subscriber.claim(), (True, remaining == 0))
                    self.assertEqual(subscriber.remaining, remaining)
                self.assertEqual(subscriber.claim(), (False, False))

    def test_load_core_respects_no_extensions_env(self) -> None:
        with patch.dict(os.environ, {"EVENTFLOW_NO_EXTENSIONS": "1"}):
            self.assertIsNone(_native._load_core())

    def test_load_core_returns_none_when_extension_missing(self) -> None:
        missing_module = ModuleNotFoundError("No module named 'flowevent._core'")
        with (
            patch.dict(os.environ, {"EVENTFLOW_NO_EXTENSIONS": "0"}),
            patch.object(_native, "import_module", side_effect=missing_module),
        ):
            self.assertIsNone(_native._load_core())

    def test_native_import_that_enables_free_threaded_gil_fails_closed(self) -> None:
        with (
            patch.dict(os.environ, {"EVENTFLOW_NO_EXTENSIONS": "0"}),
            patch.object(_native.sys, "_is_gil_enabled", side_effect=(False, True), create=True),
            patch.object(_native, "import_module", return_value=SimpleNamespace()),
            self.assertRaisesRegex(RuntimeError, "enabled the free-threaded GIL"),
        ):
            _native._load_core()

    @unittest.skipUnless(_native.CORE is not None, "flowevent._core extension is not compiled")
    def test_core_extension_powers_event_bus(self) -> None:
        core = _native.CORE
        self.assertEqual(core.__name__, "flowevent._core")

        seen: list[str] = []
        with EventBus[str](routing_key=lambda event: event) as bus:
            self.assertIsInstance(bus._core, core.BusCore)
            bus.subscribe(lambda _event: seen.append("wildcard-1"))
            bus.subscribe(lambda _event: seen.append("keyed-1"), routing_key="a")
            bus.subscribe(lambda _event: seen.append("wildcard-2"))
            bus.subscribe(lambda _event: seen.append("keyed-2"), routing_key="a", once=True)
            bus.subscribe(lambda _event: seen.append("other-key"), routing_key="b")

            first = bus.publish("a")
            second = bus.publish("a")

        self.assertEqual(
            seen,
            [
                "wildcard-1",
                "keyed-1",
                "wildcard-2",
                "keyed-2",
                "wildcard-1",
                "keyed-1",
                "wildcard-2",
            ],
        )
        self.assertEqual((first.matched, first.delivered), (4, 4))
        self.assertEqual((second.matched, second.delivered), (3, 3))
        self.assertEqual((bus.stats.published, bus.stats.delivered), (2, 7))

    @unittest.skipUnless(_native.CORE is not None, "flowevent._core extension is not compiled")
    def test_native_publish_falls_back_before_recording_side_effects(self) -> None:
        class MalformedSnapshotBus(EventBus[str]):
            malformed_next_snapshot = False

            def _subscriber_snapshot(self, event_key: Hashable | None) -> tuple[Any, ...]:
                if self.malformed_next_snapshot:
                    self.malformed_next_snapshot = False
                    return (object(),)
                return super()._subscriber_snapshot(event_key)

        history = MemoryEventHistory[str]()
        received: list[str] = []
        with MalformedSnapshotBus(history=history) as bus:
            self.assertIsNotNone(bus._core)
            bus.subscribe(received.append)
            bus.malformed_next_snapshot = True

            result = bus.publish("event")

        self.assertEqual(result.delivered, 1)
        self.assertEqual(received, ["event"])
        self.assertEqual(history.query(), ("event",))
        self.assertEqual(bus.stats.published, 1)
        self.assertEqual(bus.stats.delivered, 1)

    def test_native_publish_respects_subclass_subscribe_overrides(self) -> None:
        class CustomSubscribeBus(EventBus[str]):
            snapshot_calls = 0

            def subscribe(self, *args: Any, **kwargs: Any) -> Any:
                return super().subscribe(*args, **kwargs)

            def _subscriber_snapshot(self, event_key: Hashable | None) -> tuple[Any, ...]:
                self.snapshot_calls += 1
                return super()._subscriber_snapshot(event_key)

        received: list[str] = []
        with CustomSubscribeBus() as bus:
            bus.subscribe(received.append)
            result = bus.publish("event")

        self.assertEqual(received, ["event"])
        self.assertEqual((result.matched, result.delivered), (1, 1))
        self.assertEqual(bus.snapshot_calls, 1)
