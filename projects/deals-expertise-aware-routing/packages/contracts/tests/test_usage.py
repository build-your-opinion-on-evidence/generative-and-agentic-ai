import unittest

from deals_contracts import TokenUsage


class UsageTests(unittest.TestCase):
    def usage(self, **changes):
        fields = dict(
            call_id="call-001",
            task_id="task-001",
            run_id="run-001",
            system="deals",
            phase="test",
            stage="execution",
            model="configured-model",
            source="provider",
            input_tokens=100,
            output_tokens=20,
            cached_input_tokens=40,
        )
        return TokenUsage(**(fields | changes))

    def test_cached_tokens_are_not_double_counted(self):
        self.assertEqual(self.usage().total_tokens, 120)

    def test_missing_usage_is_not_zero(self):
        usage = self.usage(
            source="unavailable",
            input_tokens=None,
            output_tokens=None,
            cached_input_tokens=None,
        )
        self.assertIsNone(usage.total_tokens)

    def test_partial_usage_has_no_total(self):
        self.assertIsNone(self.usage(output_tokens=None).total_tokens)

    def test_measured_zero_is_preserved(self):
        usage = self.usage(
            input_tokens=0, output_tokens=0, cached_input_tokens=0
        )
        self.assertEqual(usage.total_tokens, 0)

    def test_invalid_counts_are_rejected(self):
        for value in (-1, True, 1.5):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    self.usage(output_tokens=value)

    def test_invalid_cached_counts_are_rejected(self):
        for changes in (
            {"cached_input_tokens": 101},
            {"input_tokens": None},
        ):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    self.usage(**changes)

    def test_invalid_measurement_sources_are_rejected(self):
        for changes in (
            {"source": "invalid"},
            {"source": "unavailable"},
            {
                "input_tokens": None,
                "output_tokens": None,
                "cached_input_tokens": None,
            },
        ):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    self.usage(**changes)


if __name__ == "__main__":
    unittest.main()
