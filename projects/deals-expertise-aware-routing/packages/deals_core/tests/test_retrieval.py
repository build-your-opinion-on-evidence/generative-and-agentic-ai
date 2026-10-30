import unittest
from math import sqrt

from deals_core import Trajectory, cosine, retrieve, trajectory_score


class RetrievalTests(unittest.TestCase):
    def trajectory(self, trajectory_id="t-1", **changes):
        fields = dict(
            trajectory_id=trajectory_id,
            owner_id="agent-1",
            context="Verified local execution",
            embedding=(1.0, 0.0),
            local_execution_seconds=0.0,
            verified_correct=True,
        )
        return Trajectory(**(fields | changes))

    def test_seconds_are_converted_to_minutes(self):
        self.assertAlmostEqual(trajectory_score((1, 0), (1, 0), 0), 1.2)
        self.assertAlmostEqual(trajectory_score((1, 0), (1, 0), 60), 1.1)
        self.assertAlmostEqual(
            trajectory_score((1, 0), (1, 0), 120), 1 + 0.2 / 3
        )

    def test_cosine_normalizes_non_unit_vectors(self):
        self.assertAlmostEqual(cosine((2, 0), (9, 0)), 1)
        self.assertAlmostEqual(cosine((1, 0), (0, 3)), 0)
        self.assertAlmostEqual(cosine((1, 0), (-1, 0)), -1)

    def test_threshold_applies_to_weighted_score(self):
        trajectory = self.trajectory(
            embedding=(0.65, sqrt(1 - 0.65 ** 2))
        )
        hits = retrieve("agent-1", (1, 0), [trajectory])
        self.assertEqual(len(hits), 1)
        self.assertAlmostEqual(hits[0]["score"], 0.78)

    def test_top_three_prefers_faster_positive_matches(self):
        pool = [
            self.trajectory(f"t-{i}", local_execution_seconds=60 * i)
            for i in range(4)
        ]
        hits = retrieve("agent-1", (1, 0), list(reversed(pool)))
        self.assertEqual(
            [h["trajectory_id"] for h in hits], ["t-0", "t-1", "t-2"]
        )

    def test_threshold_is_inclusive(self):
        hits = retrieve(
            "agent-1", (1, 0), [self.trajectory()], threshold=1.2
        )
        self.assertEqual(len(hits), 1)

    def test_memory_is_private_and_verified_only(self):
        pool = [
            self.trajectory(),
            self.trajectory("foreign", owner_id="agent-2"),
            self.trajectory("failed", verified_correct=False),
        ]
        hits = retrieve("agent-1", (1, 0), pool)
        self.assertEqual([h["trajectory_id"] for h in hits], ["t-1"])

    def test_equal_scores_use_stable_id_order(self):
        hits = retrieve(
            "agent-1", (1, 0),
            [self.trajectory("z"), self.trajectory("a")],
        )
        self.assertEqual([h["trajectory_id"] for h in hits], ["a", "z"])

    def test_invalid_vectors_and_times_are_rejected(self):
        for vector in ((0, 0), (float("nan"), 0), ()):
            with self.assertRaises(ValueError):
                cosine(vector, (1, 0))
        with self.assertRaises(ValueError):
            cosine((1, 0), (1, 0, 0))
        with self.assertRaises(ValueError):
            trajectory_score((1, 0), (1, 0), -1)

    def test_pool_limit_and_duplicate_ids_are_enforced(self):
        with self.assertRaises(ValueError):
            retrieve(
                "agent-1", (1, 0),
                [self.trajectory(str(i)) for i in range(41)],
            )
        with self.assertRaises(ValueError):
            retrieve(
                "agent-1", (1, 0),
                [self.trajectory(), self.trajectory()],
            )


if __name__ == "__main__":
    unittest.main()
