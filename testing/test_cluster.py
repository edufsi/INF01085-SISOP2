import socket
import time
import unittest

from common.protocol import decode, encode, message
from server.node import ServerNode


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


if __name__ == "__main__":
    unittest.main()
