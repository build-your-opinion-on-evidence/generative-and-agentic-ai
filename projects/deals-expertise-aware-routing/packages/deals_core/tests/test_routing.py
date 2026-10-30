import unittest

from deals_core import TypeState, backlog, quality, route


class RoutingTests(unittest.TestCase):
    def state(self, agent_id="agent-1", agent_index=1, **changes):
        fields = dict(
            agent_id=agent_id, agent_index=agent_index,
            task_type="math", queued=5, in_flight=1,
            successes=5, failures=3,
        )
        return TypeState(**(fields | changes))

    def test_dequeue_preserves_backlog(self):
        self.assertEqual(backlog(5, 1), 6)
        self.assertEqual(backlog(5, 1), backlog(4, 2))

    def test_beta_prior_and_learned_quality(self):
        self.assertEqual(quality(0, 0), 0.5)
        self.assertEqual(quality(5, 3), 0.6)
        self.assertEqual(quality(7, 1), 0.8)

    def test_hand_calculated_score_seven(self):
        neighbor = self.state(
            "agent-2", 2, queued=2, successes=7, failures=1
        )
        decision = route(self.state(), [neighbor], weight=20, hop_count=0)
        self.assertEqual(decision["destination"], "agent-2")
        self.assertEqual(decision["neighbors"][0]["score"], 7)

    def test_exact_margin_does_not_forward(self):
        neighbor = self.state("agent-2", 2, successes=7, failures=1)
        decision = route(self.state(), [neighbor], weight=5, hop_count=0)
        self.assertEqual(decision["neighbors"][0]["score_exact"], "1")
        self.assertEqual(decision["action"], "local")

    def test_previous_agent_is_excluded(self):
        neighbor = self.state("agent-2", 2, queued=0, in_flight=0)
        decision = route(
            self.state(), [neighbor], weight=0,
            hop_count=1, previous_agent="agent-2",
        )
        self.assertEqual(decision["action"], "local")
        self.assertEqual(decision["neighbors"], [])

    def test_hop_limit_blocks_forwarding(self):
        neighbor = self.state("agent-2", 2, queued=0, in_flight=0)
        decision = route(self.state(), [neighbor], weight=80, hop_count=3)
        self.assertEqual(decision["reason"], "hop_limit")
        self.assertIsNone(decision["destination"])

    def test_tie_uses_numeric_agent_index(self):
        neighbors = [
            self.state("agent-10", 10, queued=0, in_flight=0),
            self.state("agent-2", 2, queued=0, in_flight=0),
        ]
        decision = route(self.state(), neighbors, weight=0, hop_count=0)
        self.assertEqual(decision["destination"], "agent-2")

    def test_quality_can_outweigh_a_larger_backlog(self):
        local = self.state(queued=1, successes=0, failures=0)
        neighbor = self.state(
            "agent-2", 2, queued=3, successes=7, failures=1
        )
        for weight, expected in ((0, "local"), (20, "forward")):
            with self.subTest(weight=weight):
                decision = route(
                    local, [neighbor], weight=weight, hop_count=0
                )
                self.assertEqual(decision["action"], expected)

    def test_invalid_inputs_are_rejected(self):
        for value in (-1, True, 1.5):
            with self.assertRaises(ValueError):
                backlog(value, 0)
        with self.assertRaises(ValueError):
            quality(0, 0, alpha=0)
        with self.assertRaises(ValueError):
            route(self.state(), [], weight=float("nan"), hop_count=0)
        with self.assertRaises(ValueError):
            route(
                self.state(),
                [self.state("agent-2", 2, task_type="law")],
                weight=20, hop_count=0,
            )


if __name__ == "__main__":
    unittest.main()
