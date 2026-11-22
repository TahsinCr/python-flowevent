from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from flowevent.filtering import EventFilter


class EventFilterTests(unittest.TestCase):
    def test_boolean_predicate_skips_awaitable_detection(self) -> None:
        isawaitable = Mock()
        event_filter = EventFilter(predicate=lambda event: event > 0)
        with patch(
            "flowevent.filtering.inspect",
            SimpleNamespace(isawaitable=isawaitable),
        ):
            self.assertTrue(event_filter.matches(1))
        isawaitable.assert_not_called()

    def test_predicate_must_return_bool(self) -> None:
        for result in (0, "false", None):
            event_filter = EventFilter(predicate=lambda _event: result)
            with self.subTest(result=result), self.assertRaisesRegex(TypeError, "return bool"):
                event_filter.matches(1)
