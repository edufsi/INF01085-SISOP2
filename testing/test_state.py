import unittest

from common.protocol import MAX_UINT64, ProtocolError
from server.state import ReplicatedState


class ReplicatedStateTests(unittest.TestCase):
    def test_exactly_once_and_future_request(self) -> None:
        state = ReplicatedState()
        accepted = state.apply_request("client", 1, 10, "127.0.0.1", 5000)
        duplicate = state.apply_request("client", 1, 10)
        future = state.apply_request("client", 3, 99)
        self.assertEqual(accepted["classification"], "NEW")
        self.assertEqual(duplicate["classification"], "DUPLICATE")
        self.assertEqual(future["classification"], "FUTURE")
        self.assertEqual((state.num_reqs, state.total_sum, state.state_version), (1, 10, 1))

    def test_replication_order(self) -> None:
        state = ReplicatedState()
        state.apply_replication(
            {
                "state_version": 1,
                "client_id": "c",
                "request_id": 1,
                "value": 7,
                "client_host": "127.0.0.1",
                "client_port": 1234,
            }
        )
        with self.assertRaises(ProtocolError):
            state.apply_replication(
                {
                    "state_version": 3,
                    "client_id": "c",
                    "request_id": 2,
                    "value": 8,
                }
            )

    def test_snapshot_round_trip(self) -> None:
        state = ReplicatedState(election_term=4, membership_version=2)
        state.apply_request("c", 1, 5, "127.0.0.1", 9999)
        restored = ReplicatedState.from_snapshot(state.to_snapshot())
        self.assertEqual(restored.to_snapshot(), state.to_snapshot())

    def test_overflow_is_rejected(self) -> None:
        state = ReplicatedState(total_sum=MAX_UINT64)
        with self.assertRaises(ProtocolError):
            state.apply_request("c", 1, 1)


if __name__ == "__main__":
    unittest.main()
