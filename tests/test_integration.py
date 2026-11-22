import subprocess
import sys
import unittest
from dataclasses import replace
from pathlib import Path

import flowevent
from flowevent import CallbackSubscription, EventBus, EventEnvelope, SubscriptionScope


class IntegrationTests(unittest.TestCase):
    def test_optional_exports_load_modules_on_first_access(self):
        source = """
import sys
import flowevent

modules = {f"flowevent{name}" for name in flowevent._OPTIONAL_EXPORTS.values()}
assert not modules.intersection(sys.modules)
loaded = set()
for name, module in flowevent._OPTIONAL_EXPORTS.items():
    full_name = f"flowevent{module}"
    if full_name not in loaded:
        assert name in dir(flowevent)
        getattr(flowevent, name)
        assert full_name in sys.modules
        loaded.add(full_name)
"""
        subprocess.run(
            [sys.executable, "-c", source], check=True, cwd=Path(__file__).resolve().parents[1]
        )
        with self.assertRaises(AttributeError):
            _ = flowevent.missing_export

    def test_bus_envelope_scope_and_callback_hooks(self):
        callback = CallbackSubscription(
            1, lambda: None, lambda envelope: replace(envelope, event=envelope.event + 1)
        )
        hooks = []

        @callback.on_before
        def before(context):
            hooks.append(context.event.id)

        output = []
        with EventBus[EventEnvelope[int]]() as bus, SubscriptionScope() as scope:
            handle = scope.add(bus.subscribe(lambda event: output.append(callback.invoke(event))))
            event = EventEnvelope(1, correlation_id="request")
            bus.publish(event)
        self.assertFalse(handle.active)
        self.assertEqual(output[0].event, 2)
        self.assertEqual(output[0].id, event.id)
        self.assertEqual(hooks, [event.id])
