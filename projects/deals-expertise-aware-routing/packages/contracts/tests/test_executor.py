import unittest

from deals_contracts import ExecuteOutput, SplitOutput


class ExecutorTests(unittest.TestCase):
    def test_execute_returns_final_answer(self):
        output = ExecuteOutput(final_answer="x = 3")
        self.assertEqual(output.action, "execute")
        self.assertEqual(output.final_answer, "x = 3")

    def test_split_preserves_progress_and_continuation(self):
        output = SplitOutput(
            completed_results=("Reduced equation to x = 3",),
            remaining_work="Verify the value in the original equation",
        )
        self.assertEqual(output.action, "split")
        self.assertEqual(
            output.completed_results, ("Reduced equation to x = 3",)
        )
        self.assertEqual(
            output.remaining_work,
            "Verify the value in the original equation",
        )

    def test_blank_final_answer_is_rejected(self):
        for value in ("", " ", None):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    ExecuteOutput(final_answer=value)

    def test_blank_continuation_is_rejected(self):
        with self.assertRaises(ValueError):
            SplitOutput(completed_results=("Partial answer",), remaining_work=" ")

    def test_invalid_completed_results_are_rejected(self):
        for value in (["Partial answer"], ("",), (123,)):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    SplitOutput(
                        completed_results=value,
                        remaining_work="Continue solving",
                    )

    def test_execute_cannot_include_a_split_payload(self):
        with self.assertRaises(TypeError):
            ExecuteOutput(
                final_answer="x = 3",
                remaining_work="Continue solving",
            )


if __name__ == "__main__":
    unittest.main()
