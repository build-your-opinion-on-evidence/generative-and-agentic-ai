import unittest
from dataclasses import FrozenInstanceError, replace

from deals_contracts import TaskEnvelope


class TaskTests(unittest.TestCase):
    def make_task(self, **changes):
        fields = dict(
            run_id="run-001",
            task_type="math.algebra",
            phase="warmup",
            original_prompt="Solve x + 2 = 5",
            remaining_work="Solve x + 2 = 5",
        )
        return TaskEnvelope(**(fields | changes))

    def test_new_tasks_have_unique_ids_and_empty_state(self):
        first, second = self.make_task(), self.make_task()
        self.assertNotEqual(first.task_id, second.task_id)
        self.assertEqual(first.hop_count, 0)
        self.assertEqual(first.split_count, 0)
        self.assertEqual(first.executors, ())
        self.assertIsNone(first.previous_agent)

    def test_continuation_preserves_identity_and_progress(self):
        task = self.make_task(
            hop_count=2,
            previous_agent="agent-1",
            completed_results=("x = 3",),
        )
        continuation = replace(
            task,
            remaining_work="Verify the answer",
            split_count=1,
            continuation_version=1,
        )
        for name in (
            "task_id", "run_id", "original_prompt",
            "hop_count", "previous_agent", "completed_results",
        ):
            self.assertEqual(
                getattr(task, name), getattr(continuation, name)
            )

    def test_invalid_counters_are_rejected(self):
        for value in (-1, True, 1.5):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    self.make_task(hop_count=value)

    def test_duplicate_executors_are_rejected(self):
        with self.assertRaises(ValueError):
            self.make_task(executors=("agent-1", "agent-1"))

    def test_invalid_phase_is_rejected(self):
        with self.assertRaises(ValueError):
            self.make_task(phase="invalid")

    def test_blank_prompt_is_rejected(self):
        with self.assertRaises(ValueError):
            self.make_task(original_prompt=" ")

    def test_task_cannot_be_mutated_in_place(self):
        with self.assertRaises(FrozenInstanceError):
            self.make_task().hop_count = 1


if __name__ == "__main__":
    unittest.main()
