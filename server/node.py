from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import random
import socket
import threading
import time
import uuid
from typing import Any, Callable

from common.protocol import (
    ProtocolError,
    assemble_snapshot,
    decode,
    encode,
    message,
    require_uint64,
    snapshot_chunks,
)
try:
    from .state import MemberState, ServerState
except ImportError:
    from state import MemberState, ServerState


JOIN_HELLO_INTERVAL = 0.5
HEARTBEAT_INTERVAL = 0.5
BACKUP_HEARTBEAT_INTERVAL = 0.5
FAILURE_TIMEOUT = 2.0
STARTUP_SETTLE = 1.2
ELECTION_RESPONSE_TIMEOUT = 0.7
COORDINATOR_TIMEOUT = 2.5
RETRY_INTERVAL = 0.2
STATUS_LOG_INTERVAL = 2.0


@dataclass(frozen=True)
class NodeTiming:
    join_hello_interval: float = JOIN_HELLO_INTERVAL
    heartbeat_interval: float = HEARTBEAT_INTERVAL
    backup_heartbeat_interval: float = BACKUP_HEARTBEAT_INTERVAL
    failure_timeout: float = FAILURE_TIMEOUT
    startup_settle: float = STARTUP_SETTLE
    election_response_timeout: float = ELECTION_RESPONSE_TIMEOUT
    coordinator_timeout: float = COORDINATOR_TIMEOUT
    retry_interval: float = RETRY_INTERVAL


@dataclass
class SnapshotReceiver:
    count: int
    digest: str
    sender_id: int
    chunks: dict[int, str] = field(default_factory=dict)


class ServerNode:
    def __init__(
        self,
        cluster_port: int,
        server_id: int,
        bind_host: str = "",
        *,
        broadcast_fanout: Callable[[dict[str, Any]], None] | None = None,
        event_logger: Callable[[str], None] | None = None,
        request_logging: bool = True,
        startup_logging: bool = True,
        timing: NodeTiming | None = None,
        discovery_address: tuple[str, int] | None = None,
    ) -> None:
        self.cluster_port = cluster_port
        self.advertised_host = bind_host or self._local_ip()
        self.state = ServerState(server_id, self.advertised_host, cluster_port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.socket.bind((bind_host, cluster_port))
        self.socket.settimeout(0.2)
        self.broadcast_address = ("255.255.255.255", cluster_port)
        self.broadcast_fanout = broadcast_fanout
        self.event_logger = event_logger
        self.request_logging = request_logging
        self.startup_logging = startup_logging
        self.timing = timing or NodeTiming()
        self.discovery_address = discovery_address
        self.running = threading.Event()
        self.running.set()
        self.send_lock = threading.Lock()
        self.election_lock = threading.Lock()
        self.syncing: set[int] = set()
        self.syncing_lock = threading.Lock()
        self.pending_replication: dict[tuple[int, int], threading.Event] = {}
        self.pending_snapshots: dict[tuple[str, int], threading.Event] = {}
        self.snapshot_receivers: dict[str, SnapshotReceiver] = {}
        self.snapshot_installed = threading.Event()
        self.election_ok: dict[int, threading.Event] = {}
        self.coordinator_announced = threading.Event()
        self.leave_acknowledged = threading.Event()
        self.summary_responses: dict[int, dict[int, tuple[int, tuple[str, int]]]] = {}
        self.threads: list[threading.Thread] = []
        self.started_at = time.monotonic()
        self.last_leader_heartbeat = self.started_at
        self.last_relay_registration = 0.0
        self.last_join_hello = 0.0
        self.last_primary_heartbeat = 0.0
        self.last_backup_heartbeat = 0.0
        self.last_status_log = self.started_at

    @staticmethod
    def _local_ip() -> str:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect(("8.8.8.8", 80))
            return probe.getsockname()[0]
        except OSError:
            return "127.0.0.1"
        finally:
            probe.close()

    @staticmethod
    def timestamp() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def log(self, text: str) -> None:
        line = f"{self.timestamp()} cluster {text}"
        if self.event_logger is not None:
            self.event_logger(line)
        else:
            print(line, flush=True)

    @staticmethod
    def _address_text(address: tuple[str, int]) -> str:
        return f"{address[0]}:{address[1]}"

    def _client_tracking_text(self) -> str:
        clients = sorted(self.state.replicated.clients.items())
        if not clients:
            return "[]"
        entries = [
            (
                f"{client_id}:last_req={client.last_req}:"
                f"last_num_reqs={client.last_num_reqs}:"
                f"last_total_sum={client.last_total_sum}:"
                f"addr={client.last_host}:{client.last_port}"
            )
            for client_id, client in clients
        ]
        return "[" + ",".join(entries) + "]"

    def status_log_text(self) -> str:
        with self.state.lock:
            active_ids = sorted(
                server_id
                for server_id, member in self.state.members.items()
                if member.status == "ACTIVE"
            )
            return (
                f"server_status server_id={self.state.server_id} "
                f"role={self.state.role} leader_id={self.state.leader_id} "
                f"term={self.state.replicated.election_term} "
                f"state_version={self.state.replicated.state_version} "
                f"membership_version={self.state.replicated.membership_version} "
                f"num_reqs={self.state.replicated.num_reqs} "
                f"total_sum={self.state.replicated.total_sum} "
                f"transitioning={self.state.transitioning.is_set()} "
                f"active_ids={active_ids} clients={self._client_tracking_text()}"
            )

    def log_status(self) -> None:
        self.log(self.status_log_text())

    def log_client_request_received(
        self,
        address: tuple[str, int],
        client_id: str,
        request_id: int,
        value: int,
    ) -> None:
        with self.state.lock:
            text = (
                f"client_request_received server_id={self.state.server_id} "
                f"source={self._address_text(address)} client_id={client_id} "
                f"request_id={request_id} value={value} role={self.state.role} "
                f"leader_id={self.state.leader_id} "
                f"term={self.state.replicated.election_term} "
                f"state_version={self.state.replicated.state_version}"
            )
        self.log(text)

    def log_client_request_rejected(
        self,
        reason: str,
        address: tuple[str, int],
        client_id: str,
        request_id: int,
        value: int,
        **fields: Any,
    ) -> None:
        with self.state.lock:
            parts = [
                f"client_request_rejected reason={reason}",
                f"server_id={self.state.server_id}",
                f"source={self._address_text(address)}",
                f"client_id={client_id}",
                f"request_id={request_id}",
                f"value={value}",
                f"role={self.state.role}",
                f"leader_id={self.state.leader_id}",
                f"term={self.state.replicated.election_term}",
                f"state_version={self.state.replicated.state_version}",
            ]
        parts.extend(f"{key}={value}" for key, value in fields.items())
        self.log(" ".join(parts))

    def send(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        try:
            data = encode(payload)
            with self.send_lock:
                self.socket.sendto(data, address)
        except (OSError, ProtocolError):
            if self.running.is_set():
                self.log(f"send_error type {payload.get('type')} target {address[0]}:{address[1]}")

    def broadcast(self, payload: dict[str, Any]) -> None:
        if self.broadcast_fanout is not None:
            self.broadcast_fanout(payload)
        elif self.discovery_address is not None:
            self.send(payload, self.discovery_address)
        else:
            self.send(payload, self.broadcast_address)

    def server_message(self, message_type: str, **fields: Any) -> dict[str, Any]:
        with self.state.lock:
            return message(
                message_type,
                server_id=self.state.server_id,
                host=self.state.bind_host,
                port=self.state.port,
                term=self.state.replicated.election_term,
                **fields,
            )

    def start(self) -> None:
        if self.startup_logging:
            line = (
                f"{self.timestamp()} num_reqs {self.state.replicated.num_reqs} "
                f"total_sum {self.state.replicated.total_sum}"
            )
            if self.event_logger is not None:
                self.event_logger(line)
            else:
                print(line, flush=True)
        self.threads = [
            threading.Thread(target=self.receive_loop, name="receiver", daemon=True),
            threading.Thread(target=self.maintenance_loop, name="maintenance", daemon=True),
        ]
        for thread in self.threads:
            thread.start()

    def serve_forever(self) -> None:
        self.start()
        while self.running.is_set():
            time.sleep(0.2)

    def receive_loop(self) -> None:
        while self.running.is_set():
            try:
                data, address = self.socket.recvfrom(2048)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                payload = decode(data)
                self.dispatch(payload, address)
            except (ProtocolError, KeyError, TypeError, ValueError) as exc:
                self.log(f"invalid_message from {address[0]} reason {exc}")

    def dispatch(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        message_type = payload["type"]
        if message_type == "CLIENT_REQUEST":
            threading.Thread(
                target=self.handle_client_request,
                args=(payload, address),
                daemon=True,
            ).start()
            return
        handlers = {
            "CLIENT_DISCOVERY": self.handle_client_discovery,
            "CLIENT_LEAVE": self.handle_client_leave,
            "SERVER_HELLO": self.handle_server_hello,
            "JOIN_REQUEST": self.handle_join_request,
            "HEARTBEAT": self.handle_heartbeat,
            "BACKUP_HEARTBEAT": self.handle_backup_heartbeat,
            "REPLICATION": self.handle_replication,
            "REPLICATION_ACK": self.handle_replication_ack,
            "MEMBERSHIP": self.handle_membership,
            "SNAPSHOT_CHUNK": self.handle_snapshot_chunk,
            "SNAPSHOT_ACK": self.handle_snapshot_ack,
            "SNAPSHOT_REQUEST": self.handle_snapshot_request,
            "ELECTION": self.handle_election,
            "ELECTION_OK": self.handle_election_ok,
            "STATE_SUMMARY_REQUEST": self.handle_summary_request,
            "STATE_SUMMARY": self.handle_summary,
            "COORDINATOR": self.handle_coordinator,
            "LEAVE": self.handle_server_leave,
            "LEAVE_ACK": self.handle_leave_ack,
            "STATUS_REQUEST": self.handle_status_request,
        }
        handler = handlers.get(message_type)
        if handler is not None:
            handler(payload, address)

    def maintenance_loop(self) -> None:
        while self.running.is_set():
            now = time.monotonic()
            if (
                self.discovery_address is not None
                and now - self.last_relay_registration
                >= self.timing.join_hello_interval
            ):
                self.last_relay_registration = now
                self.send(
                    message(
                        "RELAY_REGISTER",
                        role="server",
                        server_id=self.state.server_id,
                        host=self.state.bind_host,
                        port=self.state.port,
                    ),
                    self.discovery_address,
                )
            with self.state.lock:
                role = self.state.role
            if (
                role == "JOINING"
                and now - self.last_join_hello >= self.timing.join_hello_interval
            ):
                self.last_join_hello = now
                self.broadcast(
                    self.server_message(
                        "SERVER_HELLO",
                        role="JOINING",
                        state_version=self.state.replicated.state_version,
                    )
                )
            if (
                role == "PRIMARY"
                and now - self.last_primary_heartbeat
                >= self.timing.heartbeat_interval
            ):
                self.last_primary_heartbeat = now
                self.broadcast(
                    self.server_message(
                        "HEARTBEAT",
                        state_version=self.state.replicated.state_version,
                        membership=self.state.membership_payload(),
                    )
                )
            if (
                role == "BACKUP"
                and now - self.last_backup_heartbeat
                >= self.timing.backup_heartbeat_interval
            ):
                self.last_backup_heartbeat = now
                self.broadcast(
                    self.server_message(
                        "BACKUP_HEARTBEAT",
                        leader_id=self.state.leader_id,
                        state_version=self.state.replicated.state_version,
                    )
                )

            self.check_startup(now)
            self.check_failures(now)
            if now - self.last_status_log >= STATUS_LOG_INTERVAL:
                self.last_status_log = now
                self.log_status()
            time.sleep(0.1)

    def check_startup(self, now: float) -> None:
        with self.state.lock:
            if self.state.role != "JOINING" or self.state.leader_id is not None:
                return
            if now - self.started_at < self.timing.startup_settle:
                return
            known_ids = [self.state.server_id, *self.state.members]
            if self.state.server_id != max(known_ids):
                return
            self.state.role = "PRIMARY"
            self.state.leader_id = self.state.server_id
            self.state.replicated.election_term += 1
            self.state.replicated.membership_version += 1
            self.state.members[self.state.server_id] = MemberState(
                self.state.server_id,
                self.state.bind_host,
                self.state.port,
                "ACTIVE",
                now,
            )
            self.state.transitioning.set()
        self.log(f"leader {self.state.server_id} term {self.state.replicated.election_term}")
        self.announce_coordinator()

    def check_failures(self, now: float) -> None:
        with self.state.lock:
            role = self.state.role
            leader_id = self.state.leader_id
            stale = [
                server_id
                for server_id, member in self.state.members.items()
                if server_id != self.state.server_id
                and member.status == "ACTIVE"
                and now - member.last_seen > self.timing.failure_timeout
            ]
        if role == "PRIMARY":
            for server_id in stale:
                self.remove_member(server_id, "timeout")
        elif (
            leader_id is not None
            and now - self.last_leader_heartbeat > self.timing.failure_timeout
        ):
            self.start_election()

    def note_server(
        self, payload: dict[str, Any], address: tuple[str, int], default_status: str = "JOINING"
    ) -> int:
        server_id = require_uint64(payload.get("server_id"), "server_id", positive=True)
        if server_id == self.state.server_id:
            return server_id
        now = time.monotonic()
        if self.discovery_address is None:
            host = address[0]
            port = address[1]
        else:
            host = str(payload.get("host") or address[0])
            port = int(payload.get("port") or address[1])
        with self.state.lock:
            member = self.state.members.get(server_id)
            if member is None:
                member = MemberState(server_id, host, port, default_status, now)
                self.state.members[server_id] = member
            else:
                member.host = host
                member.port = port
                member.last_seen = now
        return server_id

    def handle_server_hello(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        server_id = self.note_server(payload, address)
        if server_id == self.state.server_id:
            return
        role = str(payload.get("role", "JOINING"))
        leader_id = payload.get("leader_id")
        with self.state.lock:
            if role == "PRIMARY" and isinstance(leader_id, int):
                incoming_term = int(payload.get("term", 0))
                if incoming_term >= self.state.replicated.election_term:
                    self.state.leader_id = leader_id
                    self.last_leader_heartbeat = time.monotonic()
            is_primary = self.state.role == "PRIMARY"
            is_joining = self.state.role == "JOINING"
            known_status = self.state.members.get(server_id).status
        if is_primary and (known_status != "ACTIVE" or role == "JOINING"):
            self.schedule_sync(server_id)
        elif is_joining and role == "PRIMARY":
            self.send(self.server_message("JOIN_REQUEST"), address)

    def handle_join_request(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        server_id = self.note_server(payload, address)
        with self.state.lock:
            if self.state.role != "PRIMARY":
                return
        self.schedule_sync(server_id)

    def schedule_sync(self, server_id: int) -> None:
        if server_id == self.state.server_id:
            return
        with self.syncing_lock:
            if server_id in self.syncing:
                return
            self.syncing.add(server_id)
        threading.Thread(target=self.sync_member, args=(server_id,), daemon=True).start()

    def sync_member(self, server_id: int) -> None:
        try:
            with self.state.commit_lock:
                with self.state.lock:
                    if self.state.role != "PRIMARY":
                        return
                    member = self.state.members.get(server_id)
                    if member is None:
                        return
                    self.state.transitioning.clear()
                    address = member.address
                if not self.send_snapshot(server_id, address):
                    with self.state.lock:
                        self.state.members.pop(server_id, None)
                    return
                with self.state.lock:
                    member = self.state.members.get(server_id)
                    if member is None or self.state.role != "PRIMARY":
                        return
                    member.status = "ACTIVE"
                    member.last_seen = time.monotonic()
                    self.state.replicated.membership_version += 1
                self.broadcast_membership()
                self.log(f"member_active {server_id}")
                with self.state.lock:
                    should_elect = server_id > self.state.server_id
            if should_elect:
                self.send(self.server_message("ELECTION", candidate_id=server_id), address)
        finally:
            self.state.transitioning.set()
            with self.syncing_lock:
                self.syncing.discard(server_id)

    def snapshot_payload(self) -> dict[str, Any]:
        with self.state.lock:
            return {
                "replicated": self.state.replicated.to_snapshot(),
                "members": self.state.membership_payload(),
                "leader_id": self.state.leader_id,
            }

    def send_snapshot(self, target_id: int, address: tuple[str, int]) -> bool:
        transfer_id = uuid.uuid4().hex
        chunks = snapshot_chunks(self.snapshot_payload(), transfer_id)
        event = threading.Event()
        self.pending_snapshots[(transfer_id, target_id)] = event
        try:
            for _ in range(8):
                for chunk in chunks:
                    chunk.update(
                        server_id=self.state.server_id,
                        host=self.state.bind_host,
                        port=self.state.port,
                        term=self.state.replicated.election_term,
                    )
                    self.send(chunk, address)
                if event.wait(self.timing.retry_interval * 2):
                    return True
            return False
        finally:
            self.pending_snapshots.pop((transfer_id, target_id), None)

    def handle_snapshot_chunk(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        sender_id = self.note_server(payload, address)
        transfer_id = str(payload["transfer_id"])
        count = int(payload["count"])
        index = int(payload["index"])
        digest = str(payload["sha256"])
        receiver = self.snapshot_receivers.get(transfer_id)
        if receiver is None:
            receiver = SnapshotReceiver(count, digest, sender_id)
            self.snapshot_receivers[transfer_id] = receiver
        if receiver.sender_id != sender_id or receiver.count != count or receiver.digest != digest:
            return
        receiver.chunks[index] = str(payload["data"])
        if len(receiver.chunks) != receiver.count:
            return
        snapshot = assemble_snapshot(receiver.chunks, receiver.count, receiver.digest)
        replicated = snapshot["replicated"]
        incoming_term = int(replicated["election_term"])
        with self.state.lock:
            if incoming_term < self.state.replicated.election_term:
                return
            local_version = self.state.replicated.state_version
        if int(replicated["state_version"]) < local_version:
            self.snapshot_receivers.pop(transfer_id, None)
            self.send(
                self.server_message("SNAPSHOT_ACK", transfer_id=transfer_id),
                address,
            )
            return
        with self.state.lock:
            was_leaving = self.state.role == "LEAVING"
            was_candidate = self.state.role == "CANDIDATE"
        self.state.install_snapshot(replicated)
        self.state.install_membership(snapshot["members"], time.monotonic())
        with self.state.lock:
            self.state.leader_id = snapshot.get("leader_id") or sender_id
            if was_leaving:
                self.state.role = "LEAVING"
            elif was_candidate:
                self.state.role = "CANDIDATE"
            else:
                self.state.role = (
                    "PRIMARY"
                    if self.state.leader_id == self.state.server_id
                    else "BACKUP"
                )
            own = self.state.members.get(self.state.server_id)
            if own is None:
                self.state.members[self.state.server_id] = MemberState(
                    self.state.server_id,
                    self.state.bind_host,
                    self.state.port,
                    "ACTIVE",
                    time.monotonic(),
                )
            else:
                own.status = "ACTIVE"
            self.last_leader_heartbeat = time.monotonic()
            self.state.transitioning.set()
        self.snapshot_receivers.pop(transfer_id, None)
        self.snapshot_installed.set()
        self.send(
            self.server_message("SNAPSHOT_ACK", transfer_id=transfer_id),
            address,
        )
        self.log(f"snapshot_installed version {self.state.replicated.state_version}")
        with self.state.lock:
            if (
                self.state.role == "BACKUP"
                and self.state.leader_id is not None
                and self.state.server_id > self.state.leader_id
            ):
                threading.Thread(target=self.start_election, daemon=True).start()

    def handle_snapshot_ack(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        server_id = self.note_server(payload, address)
        event = self.pending_snapshots.get((str(payload["transfer_id"]), server_id))
        if event is not None:
            event.set()

    def handle_snapshot_request(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        requester_id = self.note_server(payload, address)
        threading.Thread(
            target=self.send_snapshot, args=(requester_id, address), daemon=True
        ).start()

    def broadcast_membership(self) -> None:
        self.broadcast(
            self.server_message(
                "MEMBERSHIP",
                membership_version=self.state.replicated.membership_version,
                membership=self.state.membership_payload(),
                leader_id=self.state.leader_id,
            )
        )

    def handle_membership(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        sender_id = self.note_server(payload, address)
        if sender_id == self.state.server_id:
            return
        with self.state.lock:
            if sender_id != self.state.leader_id:
                return
            version = int(payload["membership_version"])
            if version < self.state.replicated.membership_version:
                return
            self.state.replicated.membership_version = version
        self.state.install_membership(payload["membership"], time.monotonic())

    def remove_member(self, server_id: int, reason: str) -> None:
        with self.state.lock:
            member = self.state.members.pop(server_id, None)
            if member is None:
                return
            self.state.replicated.membership_version += 1
        self.log(f"member_removed {server_id} reason {reason}")
        self.broadcast_membership()

    def handle_client_discovery(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        with self.state.lock:
            if self.state.role != "PRIMARY" or not self.state.transitioning.is_set():
                return
            response = self.server_message(
                "LEADER",
                leader_id=self.state.server_id,
                state_version=self.state.replicated.state_version,
                client_id=payload.get("client_id"),
            )
        self.send(response, address)

    def handle_status_request(
        self, payload: dict[str, Any], address: tuple[str, int]
    ) -> None:
        with self.state.lock:
            clients = self.state.replicated.to_snapshot()["clients"]
            clients_hash = hashlib.sha256(
                json.dumps(
                    clients, separators=(",", ":"), sort_keys=True
                ).encode("utf-8")
            ).hexdigest()
            active_ids = sorted(
                server_id
                for server_id, member in self.state.members.items()
                if member.status == "ACTIVE"
            )
            response = message(
                "STATUS_RESPONSE",
                request_id=payload.get("request_id"),
                component="server",
                server_id=self.state.server_id,
                role=self.state.role,
                leader_id=self.state.leader_id,
                active_server_ids=active_ids,
                num_reqs=self.state.replicated.num_reqs,
                total_sum=self.state.replicated.total_sum,
                state_version=self.state.replicated.state_version,
                election_term=self.state.replicated.election_term,
                membership_version=self.state.replicated.membership_version,
                clients_hash=clients_hash,
            )
        self.send(response, address)

    def handle_client_request(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        try:
            client_id = str(payload["client_id"])
            if not client_id:
                raise ProtocolError("client_id is required")
            request_id = require_uint64(payload.get("request_id"), "request_id", positive=True)
            value = require_uint64(payload.get("value"), "value", positive=True)
        except ProtocolError as exc:
            self.log(
                f"client_request_invalid source={self._address_text(address)} "
                f"reason={exc}"
            )
            self.send(message("ERROR", reason=str(exc)), address)
            return
        self.log_client_request_received(address, client_id, request_id, value)
        with self.state.lock:
            if self.state.role != "PRIMARY":
                self.log_client_request_rejected(
                    "not_primary", address, client_id, request_id, value
                )
                self.send_not_leader(address)
                return
        if not self.state.transitioning.wait(0.5):
            self.log_client_request_rejected(
                "cluster_transition", address, client_id, request_id, value
            )
            self.send(self.server_message("RETRY", reason="cluster_transition"), address)
            return
        with self.state.commit_lock:
            with self.state.lock:
                if self.state.role != "PRIMARY":
                    self.log_client_request_rejected(
                        "not_primary_after_commit_lock",
                        address,
                        client_id,
                        request_id,
                        value,
                    )
                    self.send_not_leader(address)
                    return
                classification, client = self.state.replicated.classify_request(
                    client_id, request_id
                )
                if classification != "NEW":
                    self.log_client_request_rejected(
                        classification.lower(),
                        address,
                        client_id,
                        request_id,
                        value,
                        classification=classification,
                        stored_last_req=client.last_req,
                        stored_num_reqs=client.last_num_reqs,
                        stored_total_sum=client.last_total_sum,
                    )
                    self.reply_client(
                        address,
                        client_id,
                        client.last_req,
                        client.last_num_reqs,
                        client.last_total_sum,
                        classification,
                    )
                    return
                result = self.state.replicated.apply_request(
                    client_id, request_id, value, address[0], address[1]
                )
                operation = self.server_message(
                    "REPLICATION",
                    state_version=result["state_version"],
                    client_id=client_id,
                    request_id=request_id,
                    value=value,
                    client_host=address[0],
                    client_port=address[1],
                )
                backups = self.state.active_members()
            if not self.replicate_to_all(operation, backups):
                self.log_client_request_rejected(
                    "replication_interrupted",
                    address,
                    client_id,
                    request_id,
                    value,
                    operation_state_version=operation["state_version"],
                )
                self.send(self.server_message("RETRY", reason="replication_interrupted"), address)
                return
            self.reply_client(
                address,
                client_id,
                request_id,
                result["num_reqs"],
                result["total_sum"],
                "NEW",
            )
            if self.request_logging:
                print(
                    f"{self.timestamp()} client {address[0]} id_req {request_id} "
                    f"value {value} num_reqs {result['num_reqs']} "
                    f"total_sum {result['total_sum']}",
                    flush=True,
                )

    def replicate_to_all(
        self, operation: dict[str, Any], backups: list[MemberState]
    ) -> bool:
        version = int(operation["state_version"])
        operation_term = int(operation["term"])
        pending: dict[int, tuple[threading.Event, tuple[str, int]]] = {}
        for backup in backups:
            event = threading.Event()
            self.pending_replication[(version, backup.server_id)] = event
            pending[backup.server_id] = (event, backup.address)
            self.send(operation, backup.address)
        try:
            while pending and self.running.is_set():
                with self.state.lock:
                    if (
                        self.state.role != "PRIMARY"
                        or self.state.replicated.election_term != operation_term
                    ):
                        return False
                    active_ids = {member.server_id for member in self.state.active_members()}
                for server_id, (event, target) in list(pending.items()):
                    if event.wait(self.timing.retry_interval):
                        pending.pop(server_id, None)
                    elif server_id not in active_ids:
                        pending.pop(server_id, None)
                    else:
                        self.send(operation, target)
            return not pending
        finally:
            for server_id in pending:
                self.pending_replication.pop((version, server_id), None)
            for backup in backups:
                self.pending_replication.pop((version, backup.server_id), None)

    def handle_replication(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        sender_id = self.note_server(payload, address, "ACTIVE")
        incoming_term = int(payload.get("term", 0))
        incoming_version = int(payload.get("state_version", 0))
        with self.state.lock:
            reject_reason = None
            if sender_id != self.state.leader_id:
                reject_reason = "sender_not_leader"
            elif incoming_term < self.state.replicated.election_term:
                reject_reason = "stale_term"
            elif self.state.role not in {"BACKUP", "JOINING"}:
                reject_reason = "invalid_role"
            if reject_reason is not None:
                self.log(
                    f"replication_rejected reason={reject_reason} "
                    f"server_id={self.state.server_id} "
                    f"source={self._address_text(address)} sender_id={sender_id} "
                    f"incoming_term={incoming_term} "
                    f"incoming_state_version={incoming_version} "
                    f"local_role={self.state.role} "
                    f"local_leader_id={self.state.leader_id} "
                    f"local_term={self.state.replicated.election_term} "
                    f"local_state_version={self.state.replicated.state_version}"
                )
                return
            try:
                self.state.replicated.apply_replication(payload)
            except ProtocolError as exc:
                self.log(
                    f"replication_rejected reason=protocol_error "
                    f"server_id={self.state.server_id} "
                    f"source={self._address_text(address)} sender_id={sender_id} "
                    f"incoming_term={incoming_term} "
                    f"incoming_state_version={incoming_version} "
                    f"local_role={self.state.role} "
                    f"local_leader_id={self.state.leader_id} "
                    f"local_term={self.state.replicated.election_term} "
                    f"local_state_version={self.state.replicated.state_version} "
                    f"error={exc}"
                )
                self.send(self.server_message("SNAPSHOT_REQUEST"), address)
                return
            version = self.state.replicated.state_version
            self.log(
                f"replication_accepted server_id={self.state.server_id} "
                f"source={self._address_text(address)} sender_id={sender_id} "
                f"incoming_term={incoming_term} "
                f"incoming_state_version={incoming_version} "
                f"local_role={self.state.role} "
                f"local_leader_id={self.state.leader_id} "
                f"local_term={self.state.replicated.election_term} "
                f"local_state_version={version}"
            )
        self.send(self.server_message("REPLICATION_ACK", state_version=version), address)

    def handle_replication_ack(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        server_id = require_uint64(payload.get("server_id"), "server_id", positive=True)
        version = int(payload["state_version"])
        incoming_term = int(payload.get("term", 0))
        with self.state.lock:
            member = self.state.members.get(server_id)
            if (
                self.state.role != "PRIMARY"
                or incoming_term != self.state.replicated.election_term
                or member is None
                or member.status != "ACTIVE"
            ):
                return
            member.host = str(payload.get("host") or address[0])
            member.port = int(payload.get("port") or address[1])
            member.last_seen = time.monotonic()
        event = self.pending_replication.get((version, server_id))
        if event is not None:
            event.set()

    def reply_client(
        self,
        address: tuple[str, int],
        client_id: str,
        request_id: int,
        num_reqs: int,
        total_sum: int,
        classification: str,
    ) -> None:
        self.send(
            self.server_message(
                "CLIENT_ACK",
                client_id=client_id,
                request_id=request_id,
                num_reqs=num_reqs,
                total_sum=total_sum,
                duplicate=classification == "DUPLICATE",
                leader_id=self.state.server_id,
            ),
            address,
        )

    def send_not_leader(self, address: tuple[str, int]) -> None:
        with self.state.lock:
            leader = self.state.members.get(self.state.leader_id or -1)
            fields: dict[str, Any] = {"leader_id": self.state.leader_id}
            if leader is not None:
                fields.update(leader_host=leader.host, leader_port=leader.port)
        self.send(self.server_message("NOT_LEADER", **fields), address)

    def handle_client_leave(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        self.log(f"client_leave {payload.get('client_id', 'unknown')}")

    def handle_primary_conflict(
        self, incoming_term: int, incoming_leader_id: int, *, coordinator: bool = False
    ) -> bool:
        with self.state.lock:
            if self.state.role != "PRIMARY":
                return False
            if incoming_term < self.state.replicated.election_term:
                return True
            if coordinator and incoming_leader_id > self.state.server_id:
                return False
        threading.Thread(target=self.start_election, daemon=True).start()
        return True

    def handle_heartbeat(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        server_id = self.note_server(payload, address, "ACTIVE")
        if server_id == self.state.server_id:
            return

        incoming_term = int(payload.get("term", 0))
        if self.handle_primary_conflict(incoming_term, server_id):
            return

        with self.state.lock:
            if incoming_term < self.state.replicated.election_term:
                return

            self.state.replicated.election_term = incoming_term
            self.state.leader_id = server_id
            if (
                server_id != self.state.server_id
                and self.state.role != "LEAVING"
            ):
                self.state.role = "BACKUP" if self.state.role != "JOINING" else "JOINING"
            self.last_leader_heartbeat = time.monotonic()

        if "membership" in payload and self.state.role != "JOINING":
            self.state.install_membership(payload["membership"], time.monotonic())

    def handle_backup_heartbeat(
        self, payload: dict[str, Any], address: tuple[str, int]
    ) -> None:
        server_id = require_uint64(payload.get("server_id"), "server_id", positive=True)
        incoming_term = int(payload.get("term", 0))
        with self.state.lock:
            member = self.state.members.get(server_id)
            if (
                self.state.role != "PRIMARY"
                or incoming_term != self.state.replicated.election_term
                or member is None
                or member.status != "ACTIVE"
            ):
                return
            member.host = str(payload.get("host") or address[0])
            member.port = int(payload.get("port") or address[1])
            member.last_seen = time.monotonic()

    def start_election(self) -> None:
        if not self.running.is_set() or not self.election_lock.acquire(blocking=False):
            return
        try:
            with self.state.commit_lock:
                with self.state.lock:
                    if self.state.role in {"JOINING", "LEAVING"}:
                        return
                    self.state.transitioning.clear()
                    self.state.role = "CANDIDATE"
                    self.state.replicated.election_term += 1
                    term = self.state.replicated.election_term
                    higher = [
                        member
                        for member in self.state.active_members()
                        if member.server_id > self.state.server_id
                    ]
                    ok_event = threading.Event()
                    self.election_ok[term] = ok_event
                    self.coordinator_announced.clear()
            for member in higher:
                self.send(
                    self.server_message("ELECTION", candidate_id=self.state.server_id),
                    member.address,
                )
            if higher and ok_event.wait(self.timing.election_response_timeout):
                if not self.coordinator_announced.wait(
                    self.timing.coordinator_timeout
                ):
                    threading.Timer(0.1, self.start_election).start()
                return
            self.become_coordinator(term)
        finally:
            self.election_ok.pop(locals().get("term", -1), None)
            self.election_lock.release()

    def handle_election(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        candidate_id = int(payload.get("candidate_id", payload.get("server_id", 0)))
        incoming_term = int(payload.get("term", 0))
        self.note_server(payload, address, "ACTIVE")
        with self.state.lock:
            if incoming_term > self.state.replicated.election_term:
                self.state.replicated.election_term = incoming_term
            eligible = (
                self.state.role not in {"JOINING", "LEAVING"}
                and self.state.server_id > candidate_id
            )
        if eligible:
            response = message(
                "ELECTION_OK",
                server_id=self.state.server_id,
                host=self.state.bind_host,
                port=self.state.port,
                term=incoming_term,
                candidate_id=candidate_id,
            )
            self.send(response, address)
            threading.Thread(target=self.start_election, daemon=True).start()

    def handle_election_ok(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        self.note_server(payload, address, "ACTIVE")
        event = self.election_ok.get(int(payload.get("term", 0)))
        if event is not None:
            event.set()

    def become_coordinator(self, term: int) -> None:
        with self.state.lock:
            if self.state.role != "CANDIDATE" or self.state.replicated.election_term != term:
                return
            self.summary_responses[term] = {
                self.state.server_id: (
                    self.state.replicated.state_version,
                    self.state.address,
                )
            }
            peers = self.state.active_members()
        for peer in peers:
            self.send(self.server_message("STATE_SUMMARY_REQUEST"), peer.address)
        time.sleep(self.timing.election_response_timeout)
        summaries = self.summary_responses.pop(term, {})
        source_id, (_, source_address) = max(
            summaries.items(), key=lambda item: (item[1][0], item[0])
        )
        if source_id != self.state.server_id:
            self.snapshot_installed.clear()
            self.send(self.server_message("SNAPSHOT_REQUEST"), source_address)
            if not self.snapshot_installed.wait(self.timing.coordinator_timeout):
                threading.Timer(0.1, self.start_election).start()
                return
        with self.state.lock:
            if self.state.role != "CANDIDATE" or self.state.replicated.election_term != term:
                return
            self.state.leader_id = self.state.server_id
            self.state.replicated.election_term = max(
                self.state.replicated.election_term, term
            )
            self_member = self.state.members.setdefault(
                self.state.server_id,
                MemberState(
                    self.state.server_id,
                    self.state.bind_host,
                    self.state.port,
                    "ACTIVE",
                    time.monotonic(),
                ),
            )
            self_member.status = "ACTIVE"
            now = time.monotonic()
            stale_ids = [
                member.server_id
                for member in self.state.active_members()
                if now - member.last_seen > self.timing.failure_timeout
            ]
            for server_id in stale_ids:
                self.state.members.pop(server_id, None)
            if stale_ids:
                self.state.replicated.membership_version += 1
            peers = self.state.active_members()
        for peer in peers:
            if peer.server_id != self.state.server_id:
                self.send_snapshot(peer.server_id, peer.address)
        with self.state.lock:
            self.state.role = "PRIMARY"
            self.state.transitioning.set()
        self.announce_coordinator()
        self.log(f"leader {self.state.server_id} term {term}")

    def handle_summary_request(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        self.note_server(payload, address, "ACTIVE")
        self.send(
            self.server_message(
                "STATE_SUMMARY", state_version=self.state.replicated.state_version
            ),
            address,
        )

    def handle_summary(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        server_id = self.note_server(payload, address, "ACTIVE")
        term = int(payload.get("term", 0))
        responses = self.summary_responses.get(term)
        if responses is not None:
            responses[server_id] = (int(payload["state_version"]), address)

    def announce_coordinator(self) -> None:
        announcement = self.server_message(
            "COORDINATOR",
            leader_id=self.state.server_id,
            state_version=self.state.replicated.state_version,
            membership=self.state.membership_payload(),
        )
        self.broadcast(announcement)
        leader_notice = self.server_message(
            "LEADER",
            leader_id=self.state.server_id,
            state_version=self.state.replicated.state_version,
        )
        with self.state.lock:
            client_addresses = {
                (client.last_host, client.last_port)
                for client in self.state.replicated.clients.values()
                if client.last_host and client.last_port
            }
        for client_address in client_addresses:
            self.send(leader_notice, client_address)

    def handle_coordinator(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        leader_id = self.note_server(payload, address, "ACTIVE")
        if leader_id == self.state.server_id:
            return
        incoming_term = int(payload.get("term", 0))
        if self.handle_primary_conflict(incoming_term, leader_id, coordinator=True):
            return

        with self.state.lock:
            if incoming_term < self.state.replicated.election_term:
                local_term = self.state.replicated.election_term
                local_role = self.state.role
                local_version = self.state.replicated.state_version
                self.log(
                    f"coordinator_rejected reason=stale_coordinator "
                    f"leader_id={leader_id} incoming_term={incoming_term} "
                    f"local_term={local_term} local_role={local_role} "
                    f"local_state_version={local_version}"
                )
                return
            self.state.replicated.election_term = incoming_term
            self.state.leader_id = leader_id
            if (
                leader_id != self.state.server_id
                and self.state.role not in {"JOINING", "LEAVING"}
            ):
                self.state.role = "BACKUP"
            self.last_leader_heartbeat = time.monotonic()
            self.state.transitioning.set()
        if "membership" in payload and self.state.role != "JOINING":
            self.state.install_membership(payload["membership"], time.monotonic())
        self.coordinator_announced.set()
        with self.state.lock:
            self.log(
                f"coordinator_accepted leader_id={leader_id} "
                f"term={self.state.replicated.election_term} "
                f"role={self.state.role} "
                f"local_state_version={self.state.replicated.state_version} "
                f"leader_state_version={payload.get('state_version')}"
            )

    def handle_server_leave(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        server_id = self.note_server(payload, address, "ACTIVE")
        if server_id == self.state.server_id:
            return
        with self.state.lock:
            if self.state.role == "PRIMARY":
                self.remove_member(server_id, "graceful")
                self.send(
                    self.server_message("LEAVE_ACK", departing_id=server_id),
                    address,
                )
            elif server_id == self.state.leader_id:
                threading.Thread(target=self.start_election, daemon=True).start()

    def handle_leave_ack(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        self.note_server(payload, address, "ACTIVE")
        if int(payload.get("departing_id", 0)) == self.state.server_id:
            self.leave_acknowledged.set()

    def stop(self, graceful: bool = True, force: bool = False) -> bool:
        if not self.running.is_set():
            return True
        with self.state.lock:
            active_others = self.state.active_members()
            role = self.state.role
        if graceful and not force and not active_others:
            self.log("shutdown_refused final_active_server")
            return False
        if graceful:
            leave = self.server_message("LEAVE", role=role)
            if role == "PRIMARY":
                with self.state.lock:
                    self.state.role = "LEAVING"
                    self.state.leader_id = None
                    self.state.transitioning.clear()
                self.coordinator_announced.clear()
                for member in active_others:
                    self.send(leave, member.address)
                if not self.coordinator_announced.wait(
                    self.timing.coordinator_timeout
                    + self.timing.election_response_timeout
                ):
                    self.log("shutdown_delayed no_replacement_leader")
                    return False
            else:
                self.leave_acknowledged.clear()
                leader = next(
                    (
                        member
                        for member in active_others
                        if member.server_id == self.state.leader_id
                    ),
                    None,
                )
                deadline = time.monotonic() + self.timing.failure_timeout
                while (
                    not self.leave_acknowledged.is_set()
                    and time.monotonic() < deadline
                ):
                    if leader is not None:
                        self.send(leave, leader.address)
                    else:
                        self.broadcast(leave)
                    self.leave_acknowledged.wait(self.timing.retry_interval)
                if not self.leave_acknowledged.is_set():
                    self.log("shutdown_delayed no_leave_ack")
                    return False
        self.running.clear()
        try:
            self.socket.close()
        except OSError:
            pass
        return True


def random_server_id() -> int:
    return random.SystemRandom().randint(1, (1 << 64) - 1)
