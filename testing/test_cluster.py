import socket
import threading
import time
import unittest

from common.protocol import decode, encode, message, snapshot_chunks
from server.node import NodeTiming, ServerNode
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

    @staticmethod
    def wire_direct(nodes: list[ServerNode], client_messages: list[dict] | None = None) -> None:
        by_address = {node.state.address: node for node in nodes}

        for node in nodes:
            def direct_send(payload, address, *, sender=node):
                target = by_address.get(address)
                if target is None:
                    if client_messages is not None:
                        client_messages.append(payload)
                    return
                target.dispatch(payload, sender.state.address)

            def direct_broadcast(payload, *, sender=node):
                for target in nodes:
                    if target is not sender:
                        sender.send(payload, target.state.address)

            node.send = direct_send
            node.broadcast = direct_broadcast

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

    def test_two_primary_heartbeat_conflict_elects_highest_server(self) -> None:
        first = self.add_node(1)
        second = self.add_node(2)
        wait_until(lambda: second.state.role == "PRIMARY" and first.state.role == "BACKUP")

        with second.state.lock:
            term = second.state.replicated.election_term
        with first.state.lock:
            first.state.role = "PRIMARY"
            first.state.leader_id = 1
            first.state.replicated.election_term = term

        first.handle_heartbeat(
            second.server_message(
                "HEARTBEAT",
                state_version=second.state.replicated.state_version,
                membership=second.state.membership_payload(),
            ),
            second.state.address,
        )

        wait_until(
            lambda: second.state.role == "PRIMARY"
            and first.state.role == "BACKUP"
            and first.state.leader_id == 2,
            timeout=10.0,
        )

    def test_lower_id_coordinator_conflict_elects_highest_server(self) -> None:
        first = self.add_node(1)
        second = self.add_node(2)
        wait_until(lambda: second.state.role == "PRIMARY" and first.state.role == "BACKUP")

        with second.state.lock:
            term = second.state.replicated.election_term
        with first.state.lock:
            first.state.role = "PRIMARY"
            first.state.leader_id = 1
            first.state.replicated.election_term = term

        second.handle_coordinator(
            first.server_message(
                "COORDINATOR",
                leader_id=1,
                state_version=first.state.replicated.state_version,
                membership=first.state.membership_payload(),
            ),
            first.state.address,
        )

        wait_until(
            lambda: second.state.role == "PRIMARY"
            and first.state.role == "BACKUP"
            and first.state.leader_id == 2,
            timeout=10.0,
        )

    def test_stale_candidate_cannot_become_coordinator_after_newer_coordinator(self) -> None:
        candidate = ServerNode(self.base_port + 100, 1, "127.0.0.1")
        self.nodes.append(candidate)
        with candidate.state.lock:
            candidate.state.role = "CANDIDATE"
            candidate.state.leader_id = None
            candidate.state.replicated.election_term = 2

        candidate.handle_coordinator(
            message(
                "COORDINATOR",
                server_id=2,
                host="127.0.0.1",
                port=self.base_port + 101,
                term=3,
                leader_id=2,
                state_version=0,
                membership=[],
            ),
            ("127.0.0.1", self.base_port + 101),
        )
        candidate.become_coordinator(2)

        with candidate.state.lock:
            self.assertEqual(candidate.state.role, "BACKUP")
            self.assertEqual(candidate.state.leader_id, 2)
            self.assertEqual(candidate.state.replicated.election_term, 3)

    def test_election_ok_uses_candidate_term(self) -> None:
        responder = ServerNode(self.base_port + 110, 2, "127.0.0.1")
        self.nodes.append(responder)
        sent: list[dict] = []

        def record_send(payload, _address):
            sent.append(payload)

        responder.send = record_send
        responder.start_election = lambda: None
        with responder.state.lock:
            responder.state.role = "PRIMARY"
            responder.state.leader_id = 2
            responder.state.replicated.election_term = 5

        responder.handle_election(
            message(
                "ELECTION",
                server_id=1,
                host="127.0.0.1",
                port=self.base_port + 111,
                term=3,
                candidate_id=1,
            ),
            ("127.0.0.1", self.base_port + 111),
        )

        self.assertEqual(sent[0]["type"], "ELECTION_OK")
        self.assertEqual(sent[0]["term"], 3)
        self.assertEqual(sent[0]["candidate_id"], 1)

    def test_direct_peer_tracking_prefers_packet_source_over_stale_advertised_host(self) -> None:
        node = ServerNode(self.base_port + 120, 1, "127.0.0.1")
        self.nodes.append(node)

        node.note_server(
            message(
                "HEARTBEAT",
                server_id=2,
                host="10.0.0.5",
                port=45000,
                term=1,
                state_version=0,
            ),
            ("10.0.0.99", 45000),
            "ACTIVE",
        )

        with node.state.lock:
            self.assertEqual(node.state.members[2].address, ("10.0.0.99", 45000))

    def test_relay_peer_tracking_keeps_advertised_server_endpoint(self) -> None:
        node = ServerNode(
            self.base_port + 130,
            1,
            "127.0.0.1",
            discovery_address=("127.0.0.1", self.base_port + 131),
        )
        self.nodes.append(node)

        node.note_server(
            message(
                "HEARTBEAT",
                server_id=2,
                host="10.0.0.5",
                port=45000,
                term=1,
                state_version=0,
            ),
            ("127.0.0.1", self.base_port + 131),
            "ACTIVE",
        )

        with node.state.lock:
            self.assertEqual(node.state.members[2].address, ("10.0.0.5", 45000))

    def test_split_rejoin_elected_leader_catches_up_before_serving_client(self) -> None:
        fresh = ServerNode(
            self.base_port + 140,
            1,
            "127.0.0.1",
            request_logging=False,
            startup_logging=False,
        )
        stale = ServerNode(
            self.base_port + 141,
            2,
            "127.0.0.1",
            request_logging=False,
            startup_logging=False,
        )
        self.nodes.extend([fresh, stale])
        client_messages: list[dict] = []
        self.wire_direct([fresh, stale], client_messages)
        now = time.monotonic()
        with fresh.state.lock:
            fresh.state.role = "PRIMARY"
            fresh.state.leader_id = 1
            fresh.state.replicated.election_term = 5
            fresh.state.members[1] = MemberState(1, "127.0.0.1", fresh.state.port, "ACTIVE", now)
            fresh.state.members[2] = MemberState(2, "127.0.0.1", stale.state.port, "ACTIVE", now)
            for request_id in range(1, 32):
                fresh.state.replicated.apply_request(
                    "client-a", request_id, 1, "127.0.0.1", self.base_port + 500
                )
        with stale.state.lock:
            stale.state.role = "PRIMARY"
            stale.state.leader_id = 2
            stale.state.replicated.election_term = 5
            stale.state.members[1] = MemberState(1, "127.0.0.1", fresh.state.port, "ACTIVE", now)
            stale.state.members[2] = MemberState(2, "127.0.0.1", stale.state.port, "ACTIVE", now)
            for request_id in range(1, 21):
                stale.state.replicated.apply_request(
                    "client-a", request_id, 1, "127.0.0.1", self.base_port + 500
                )

        stale.start_election()
        wait_until(
            lambda: stale.state.role == "PRIMARY"
            and stale.state.replicated.state_version == 31
            and fresh.state.role == "BACKUP"
            and fresh.state.leader_id == 2,
            timeout=5.0,
        )

        stale.handle_client_request(
            message("CLIENT_REQUEST", client_id="client-a", request_id=32, value=1),
            ("127.0.0.1", self.base_port + 500),
        )

        self.assertEqual(client_messages[-1]["type"], "CLIENT_ACK")
        self.assertEqual(client_messages[-1]["request_id"], 32)
        self.assertEqual(client_messages[-1]["num_reqs"], 32)
        wait_until(lambda: fresh.state.replicated.state_version == 32)

    def test_summary_response_matches_candidate_summary_term(self) -> None:
        node = ServerNode(self.base_port + 142, 1, "127.0.0.1")
        self.nodes.append(node)
        with node.state.lock:
            node.summary_responses[8] = {}

        node.handle_summary(
            message(
                "STATE_SUMMARY",
                server_id=2,
                host="127.0.0.1",
                port=self.base_port + 143,
                term=3,
                summary_term=8,
                state_version=12,
            ),
            ("127.0.0.1", self.base_port + 143),
        )

        with node.state.lock:
            self.assertEqual(
                node.summary_responses[8][2],
                (12, ("127.0.0.1", self.base_port + 143)),
            )

    def test_candidate_installs_fresher_snapshot_with_lower_term(self) -> None:
        node = ServerNode(self.base_port + 144, 1, "127.0.0.1")
        self.nodes.append(node)
        sent: list[dict] = []
        node.send = lambda payload, _address: sent.append(payload)
        with node.state.lock:
            node.state.role = "CANDIDATE"
            node.state.leader_id = None
            node.state.replicated.election_term = 7

        snapshot = {
            "replicated": {
                "num_reqs": 1,
                "total_sum": 9,
                "state_version": 1,
                "election_term": 3,
                "membership_version": 1,
                "clients": {
                    "client-a": {
                        "last_req": 1,
                        "last_num_reqs": 1,
                        "last_total_sum": 9,
                        "last_value": 9,
                        "last_host": "127.0.0.1",
                        "last_port": self.base_port + 145,
                    }
                },
            },
            "members": [],
            "leader_id": 2,
        }
        for chunk in snapshot_chunks(snapshot, "snapshot-lower-term"):
            chunk.update(
                server_id=2,
                host="127.0.0.1",
                port=self.base_port + 146,
                term=3,
            )
            node.handle_snapshot_chunk(chunk, ("127.0.0.1", self.base_port + 146))

        with node.state.lock:
            self.assertEqual(node.state.role, "CANDIDATE")
            self.assertEqual(node.state.replicated.state_version, 1)
            self.assertEqual(node.state.replicated.election_term, 7)
            self.assertEqual(node.state.replicated.clients["client-a"].last_req, 1)
        self.assertEqual(sent[-1]["type"], "SNAPSHOT_ACK")

    def test_stale_coordinator_is_rejected_without_demoting_fresher_primary(self) -> None:
        logs: list[str] = []
        snapshot_targets: list[tuple[int, tuple[str, int]]] = []
        recovery: list[bool] = []
        node = ServerNode(
            self.base_port + 147,
            1,
            "127.0.0.1",
            event_logger=logs.append,
            request_logging=False,
            startup_logging=False,
        )
        self.nodes.append(node)
        node.send_snapshot = lambda server_id, address: snapshot_targets.append((server_id, address)) or True
        node.start_election = lambda: recovery.append(True)
        with node.state.lock:
            node.state.role = "PRIMARY"
            node.state.leader_id = 1
            node.state.replicated.election_term = 4
            node.state.members[2] = MemberState(
                2, "127.0.0.1", self.base_port + 148, "ACTIVE", time.monotonic()
            )
            for request_id in range(1, 6):
                node.state.replicated.apply_request(
                    "client-a", request_id, 1, "127.0.0.1", self.base_port + 149
                )

        node.handle_coordinator(
            message(
                "COORDINATOR",
                server_id=2,
                host="127.0.0.1",
                port=self.base_port + 148,
                term=5,
                leader_id=2,
                state_version=2,
                membership=[],
            ),
            ("127.0.0.1", self.base_port + 148),
        )

        with node.state.lock:
            self.assertEqual(node.state.role, "PRIMARY")
            self.assertEqual(node.state.leader_id, 1)
        wait_until(lambda: bool(snapshot_targets))
        wait_until(lambda: bool(recovery))
        self.assertTrue(any("coordinator_rejected reason=stale_state" in line for line in logs))

    def test_replication_timeout_returns_false_and_schedules_recovery(self) -> None:
        recovery: list[bool] = []
        primary = ServerNode(
            self.base_port + 149,
            1,
            "127.0.0.1",
            request_logging=False,
            startup_logging=False,
            timing=NodeTiming(retry_interval=0.01, replication_timeout=0.05),
        )
        self.nodes.append(primary)
        primary.send = lambda _payload, _address: None
        primary.start_election = lambda: recovery.append(True)
        backup = MemberState(
            server_id=2,
            host="127.0.0.1",
            port=self.base_port + 150,
            status="ACTIVE",
            last_seen=time.monotonic(),
        )
        with primary.state.lock:
            primary.state.role = "PRIMARY"
            primary.state.leader_id = 1
            primary.state.members[2] = backup
            operation = primary.server_message(
                "REPLICATION",
                state_version=1,
                client_id="client-a",
                request_id=1,
                value=1,
            )

        started = time.monotonic()
        self.assertFalse(primary.replicate_to_all(operation, [backup]))
        self.assertLess(time.monotonic() - started, 1.0)
        wait_until(lambda: bool(recovery))

    def test_newer_replication_ack_schedules_recovery(self) -> None:
        recovery: list[bool] = []
        node = ServerNode(self.base_port + 151, 1, "127.0.0.1")
        self.nodes.append(node)
        node.start_election = lambda: recovery.append(True)
        with node.state.lock:
            node.state.role = "PRIMARY"
            node.state.leader_id = 1
            node.state.replicated.election_term = 2
            node.state.replicated.apply_request(
                "client-a", 1, 1, "127.0.0.1", self.base_port + 152
            )
            node.state.members[2] = MemberState(
                2, "127.0.0.1", self.base_port + 153, "ACTIVE", time.monotonic()
            )

        node.handle_replication_ack(
            message(
                "REPLICATION_ACK",
                server_id=2,
                host="127.0.0.1",
                port=self.base_port + 153,
                term=2,
                state_version=5,
            ),
            ("127.0.0.1", self.base_port + 153),
        )

        wait_until(lambda: bool(recovery))

    def test_status_log_includes_client_tracking_and_terms(self) -> None:
        logs: list[str] = []
        node = ServerNode(
            self.base_port + 160,
            1,
            "127.0.0.1",
            event_logger=logs.append,
            request_logging=False,
            startup_logging=False,
        )
        self.nodes.append(node)
        with node.state.lock:
            node.state.role = "PRIMARY"
            node.state.leader_id = 1
            node.state.replicated.election_term = 3
            node.state.replicated.membership_version = 2
            node.state.members[1] = MemberState(
                1, "127.0.0.1", self.base_port + 160, "ACTIVE", time.monotonic()
            )
            node.state.members[2] = MemberState(
                2, "127.0.0.1", self.base_port + 161, "ACTIVE", time.monotonic()
            )
            node.state.replicated.apply_request(
                "client-a", 1, 7, "127.0.0.1", self.base_port + 162
            )

        node.log_status()

        self.assertIn("server_status server_id=1", logs[-1])
        self.assertIn("role=PRIMARY", logs[-1])
        self.assertIn("leader_id=1", logs[-1])
        self.assertIn("term=3", logs[-1])
        self.assertIn("state_version=1", logs[-1])
        self.assertIn("membership_version=2", logs[-1])
        self.assertIn("num_reqs=1", logs[-1])
        self.assertIn("total_sum=7", logs[-1])
        self.assertIn("active_ids=[1, 2]", logs[-1])
        self.assertIn(
            f"client-a:last_req=1:last_num_reqs=1:last_total_sum=7:"
            f"addr=127.0.0.1:{self.base_port + 162}",
            logs[-1],
        )

    def test_non_primary_client_request_logs_rejection(self) -> None:
        logs: list[str] = []
        sent: list[dict] = []
        node = ServerNode(
            self.base_port + 150,
            1,
            "127.0.0.1",
            event_logger=logs.append,
            request_logging=False,
            startup_logging=False,
        )
        self.nodes.append(node)
        node.send = lambda payload, _address: sent.append(payload)
        with node.state.lock:
            node.state.role = "BACKUP"
            node.state.leader_id = 2
            node.state.members[2] = MemberState(
                2, "127.0.0.1", self.base_port + 151, "ACTIVE", time.monotonic()
            )

        node.handle_client_request(
            message("CLIENT_REQUEST", client_id="client-a", request_id=1, value=5),
            ("127.0.0.1", self.base_port + 152),
        )

        self.assertTrue(any("client_request_received" in line for line in logs))
        self.assertTrue(
            any("client_request_rejected reason=not_primary" in line for line in logs)
        )
        self.assertEqual(sent[-1]["type"], "NOT_LEADER")

    def test_future_client_request_logs_stored_client_state(self) -> None:
        logs: list[str] = []
        sent: list[dict] = []
        node = ServerNode(
            self.base_port + 160,
            1,
            "127.0.0.1",
            event_logger=logs.append,
            request_logging=False,
            startup_logging=False,
        )
        self.nodes.append(node)
        node.send = lambda payload, _address: sent.append(payload)
        node.start_election = lambda: None
        with node.state.lock:
            node.state.role = "PRIMARY"
            node.state.leader_id = 1
            node.state.replicated.apply_request(
                "client-a", 1, 7, "127.0.0.1", self.base_port + 161
            )

        node.handle_client_request(
            message("CLIENT_REQUEST", client_id="client-a", request_id=3, value=9),
            ("127.0.0.1", self.base_port + 162),
        )

        self.assertTrue(
            any(
                "client_request_rejected reason=future" in line
                and "stored_last_req=1" in line
                and "request_id=3" in line
                for line in logs
            )
        )
        self.assertEqual(sent[-1]["type"], "RETRY")
        self.assertEqual(sent[-1]["reason"], "state_gap")

    def test_replication_interruption_logs_rejection_reason(self) -> None:
        logs: list[str] = []
        sent: list[dict] = []
        node = ServerNode(
            self.base_port + 170,
            1,
            "127.0.0.1",
            event_logger=logs.append,
            request_logging=False,
            startup_logging=False,
        )
        self.nodes.append(node)
        node.send = lambda payload, _address: sent.append(payload)
        node.replicate_to_all = lambda _operation, _backups: False
        with node.state.lock:
            node.state.role = "PRIMARY"
            node.state.leader_id = 1

        node.handle_client_request(
            message("CLIENT_REQUEST", client_id="client-a", request_id=1, value=9),
            ("127.0.0.1", self.base_port + 171),
        )

        self.assertTrue(
            any(
                "client_request_rejected reason=replication_interrupted" in line
                and "operation_state_version=1" in line
                for line in logs
            )
        )
        self.assertEqual(sent[-1]["type"], "RETRY")
        self.assertEqual(sent[-1]["reason"], "replication_interrupted")

    def test_backup_logs_discarded_replication(self) -> None:
        logs: list[str] = []
        sent: list[dict] = []
        node = ServerNode(
            self.base_port + 175,
            1,
            "127.0.0.1",
            event_logger=logs.append,
            request_logging=False,
            startup_logging=False,
        )
        self.nodes.append(node)
        node.send = lambda payload, _address: sent.append(payload)
        with node.state.lock:
            node.state.role = "BACKUP"
            node.state.leader_id = 3
            node.state.replicated.election_term = 2

        node.handle_replication(
            message(
                "REPLICATION",
                server_id=2,
                host="127.0.0.1",
                port=self.base_port + 176,
                term=2,
                state_version=1,
                client_id="client-a",
                request_id=1,
                value=9,
                client_host="127.0.0.1",
                client_port=self.base_port + 177,
            ),
            ("127.0.0.1", self.base_port + 176),
        )

        self.assertFalse(sent)
        self.assertTrue(
            any(
                "replication_rejected reason=sender_not_leader" in line
                and "sender_id=2" in line
                and "local_leader_id=3" in line
                and "incoming_state_version=1" in line
                for line in logs
            )
        )

    def test_backup_logs_accepted_coordinator(self) -> None:
        logs: list[str] = []
        node = ServerNode(
            self.base_port + 180,
            1,
            "127.0.0.1",
            event_logger=logs.append,
            request_logging=False,
            startup_logging=False,
        )
        self.nodes.append(node)
        with node.state.lock:
            node.state.role = "BACKUP"
            node.state.leader_id = None
            node.state.replicated.election_term = 1

        node.handle_coordinator(
            message(
                "COORDINATOR",
                server_id=2,
                host="127.0.0.1",
                port=self.base_port + 181,
                term=2,
                leader_id=2,
                state_version=4,
                membership=[],
            ),
            ("127.0.0.1", self.base_port + 181),
        )

        self.assertTrue(
            any(
                "coordinator_accepted leader_id=2" in line
                and "term=2" in line
                and "role=BACKUP" in line
                and "leader_state_version=4" in line
                for line in logs
            )
        )


if __name__ == "__main__":
    unittest.main()
