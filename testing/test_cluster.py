import socket
import threading
import time
import unittest

from common.protocol import decode, encode, message
from server.node import ServerNode
from server.state import MemberState


def wait_until(predicate, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("condition was not reached before timeout")


class ClusterIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.nodes: list[ServerNode] = []
        self.base_port = 45000

    def tearDown(self) -> None:
        for node in self.nodes:
            node.stop(graceful=False, force=True)

    def add_node(self, server_id: int) -> ServerNode:
        node = ServerNode(self.base_port + len(self.nodes), server_id, "127.0.0.1")
        self.nodes.append(node)

        def fanout(payload):
            for target in list(self.nodes):
                node.send(payload, target.state.address)

        node.broadcast = fanout
        node.start()
        return node

    @staticmethod
    def request(
        address: tuple[str, int], client_id: str, request_id: int, value: int
    ) -> dict:
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client.settimeout(5.0)
        try:
            request = encode(
                message(
                    "CLIENT_REQUEST",
                    client_id=client_id,
                    request_id=request_id,
                    value=value,
                )
            )
            for _ in range(10):
                client.sendto(request, address)
                try:
                    payload = decode(client.recvfrom(2048)[0])
                except socket.timeout:
                    continue
                if payload["type"] == "CLIENT_ACK":
                    return payload
                time.sleep(0.05)
            raise AssertionError("request was not acknowledged")
        finally:
            client.close()

    def test_replication_failover_and_higher_priority_join(self) -> None:
        first = self.add_node(1)
        second = self.add_node(2)
        third = self.add_node(3)
        wait_until(
            lambda: third.state.role == "PRIMARY"
            and all(
                len(node.state.active_members()) == 2
                for node in (first, second, third)
            )
        )

        original_send = third.send
        dropped_replication = False

        def drop_first_replication(payload, address):
            nonlocal dropped_replication
            if payload["type"] == "REPLICATION" and not dropped_replication:
                dropped_replication = True
                return
            original_send(payload, address)

        third.send = drop_first_replication
        ack = self.request(third.state.address, "client-a", 1, 11)
        self.assertTrue(dropped_replication)
        self.assertEqual((ack["num_reqs"], ack["total_sum"]), (1, 11))
        wait_until(
            lambda: all(node.state.replicated.state_version == 1 for node in self.nodes)
        )

        third.stop(graceful=False, force=True)
        wait_until(lambda: second.state.role == "PRIMARY")
        duplicate = self.request(second.state.address, "client-a", 1, 11)
        self.assertEqual((duplicate["num_reqs"], duplicate["total_sum"]), (1, 11))
        ack = self.request(second.state.address, "client-a", 2, 4)
        self.assertEqual((ack["num_reqs"], ack["total_sum"]), (2, 15))

        fourth = self.add_node(4)
        wait_until(lambda: fourth.state.role == "PRIMARY", timeout=10.0)
        wait_until(lambda: fourth.state.replicated.state_version == 2)
        ack = self.request(fourth.state.address, "client-b", 1, 5)
        self.assertEqual((ack["num_reqs"], ack["total_sum"]), (3, 20))
        live_nodes = [first, second, fourth]
        wait_until(
            lambda: all(
                (node.state.replicated.num_reqs, node.state.replicated.total_sum)
                == (3, 20)
                for node in live_nodes
            )
        )
        self.assertTrue(first.stop(graceful=True))
        wait_until(lambda: 1 not in fourth.state.members)
        self.assertTrue(fourth.stop(graceful=True))
        wait_until(lambda: second.state.role == "PRIMARY")

    def test_role_specific_heartbeat_traffic(self) -> None:
        first = self.add_node(1)
        second = self.add_node(2)
        third = self.add_node(3)
        wait_until(
            lambda: third.state.role == "PRIMARY"
            and first.state.role == second.state.role == "BACKUP"
        )

        observed: dict[int, list[str]] = {1: [], 2: [], 3: []}
        for node in (first, second, third):
            original_broadcast = node.broadcast

            def record(payload, *, node=node, original_broadcast=original_broadcast):
                observed[node.state.server_id].append(str(payload["type"]))
                original_broadcast(payload)

            node.broadcast = record

        time.sleep(0.8)

        self.assertIn("HEARTBEAT", observed[3])
        self.assertNotIn("SERVER_HELLO", observed[3])
        self.assertNotIn("BACKUP_HEARTBEAT", observed[3])
        for server_id in (1, 2):
            self.assertIn("BACKUP_HEARTBEAT", observed[server_id])
            self.assertNotIn("SERVER_HELLO", observed[server_id])
            self.assertNotIn("HEARTBEAT", observed[server_id])

    def test_idle_backup_failure_and_rejoin_require_fresh_join(self) -> None:
        backup = self.add_node(1)
        primary = self.add_node(2)
        wait_until(
            lambda: primary.state.role == "PRIMARY"
            and backup.state.role == "BACKUP"
            and 1 in primary.state.members
            and primary.state.members[1].status == "ACTIVE"
        )

        backup.stop(graceful=False, force=True)
        wait_until(lambda: 1 not in primary.state.members, timeout=5.0)

        primary.handle_backup_heartbeat(
            message(
                "BACKUP_HEARTBEAT",
                server_id=1,
                host="127.0.0.1",
                port=backup.state.port,
                term=primary.state.replicated.election_term,
                leader_id=2,
                state_version=backup.state.replicated.state_version,
            ),
            backup.state.address,
        )
        self.assertNotIn(1, primary.state.members)

        replacement = self.add_node(1)
        wait_until(
            lambda: replacement.state.role == "BACKUP"
            and 1 in primary.state.members
            and primary.state.members[1].status == "ACTIVE",
            timeout=10.0,
        )
        self.assertEqual(
            replacement.state.replicated.to_snapshot(),
            primary.state.replicated.to_snapshot(),
        )

    def test_replication_stops_when_election_term_changes(self) -> None:
        primary = self.add_node(1)
        with primary.state.lock:
            primary.state.role = "PRIMARY"
            primary.state.leader_id = 1
            backup = MemberState(
                server_id=2,
                host="127.0.0.1",
                port=self.base_port + 100,
                status="ACTIVE",
                last_seen=time.monotonic(),
            )
            primary.state.members[2] = backup
            operation = primary.server_message(
                "REPLICATION",
                state_version=1,
                client_id="client",
                request_id=1,
                value=1,
            )

        result: list[bool] = []
        worker = threading.Thread(
            target=lambda: result.append(primary.replicate_to_all(operation, [backup]))
        )
        worker.start()
        time.sleep(0.1)
        with primary.state.lock:
            primary.state.replicated.election_term += 1
        worker.join(timeout=1.0)

        self.assertFalse(worker.is_alive())
        self.assertEqual(result, [False])


if __name__ == "__main__":
    unittest.main()
