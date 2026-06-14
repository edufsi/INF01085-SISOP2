from dataclasses import asdict, dataclass, field
import threading
from typing import Any

from common.protocol import MAX_UINT64, ProtocolError, require_uint64


@dataclass
class ClientState:
    last_req: int = 0
    last_num_reqs: int = 0
    last_total_sum: int = 0
    last_value: int = 0
    last_host: str = ""
    last_port: int = 0


@dataclass
class MemberState:
    server_id: int
    host: str
    port: int
    status: str = "JOINING"
    last_seen: float = 0.0

    @property
    def address(self) -> tuple[str, int]:
        return self.host, self.port


@dataclass
class ReplicatedState:
    num_reqs: int = 0
    total_sum: int = 0
    state_version: int = 0
    election_term: int = 0
    membership_version: int = 0
    clients: dict[str, ClientState] = field(default_factory=dict)

    def classify_request(self, client_id: str, request_id: int) -> tuple[str, ClientState]:
        client = self.clients.get(client_id, ClientState())
        if request_id == client.last_req + 1:
            return "NEW", client
        if request_id <= client.last_req:
            return "DUPLICATE", client
        return "FUTURE", client

    def apply_request(
        self,
        client_id: str,
        request_id: int,
        value: int,
        client_host: str = "",
        client_port: int = 0,
    ) -> dict[str, Any]:
        require_uint64(request_id, "request_id", positive=True)
        require_uint64(value, "value", positive=True)
        classification, client = self.classify_request(client_id, request_id)
        if classification != "NEW":
            return {
                "classification": classification,
                "request_id": client.last_req,
                "num_reqs": client.last_num_reqs,
                "total_sum": client.last_total_sum,
                "state_version": self.state_version,
            }
        if self.num_reqs == MAX_UINT64 or self.total_sum > MAX_UINT64 - value:
            raise ProtocolError("aggregate unsigned 64-bit overflow")
        self.num_reqs += 1
        self.total_sum += value
        self.state_version += 1
        client.last_req = request_id
        client.last_num_reqs = self.num_reqs
        client.last_total_sum = self.total_sum
        client.last_value = value
        client.last_host = client_host
        client.last_port = client_port
        self.clients[client_id] = client
        return {
            "classification": "NEW",
            "request_id": request_id,
            "num_reqs": self.num_reqs,
            "total_sum": self.total_sum,
            "state_version": self.state_version,
        }

    def apply_replication(self, operation: dict[str, Any]) -> None:
        version = require_uint64(operation.get("state_version"), "state_version")
        if version <= self.state_version:
            return
        if version != self.state_version + 1:
            raise ProtocolError("replication gap")
        result = self.apply_request(
            str(operation["client_id"]),
            require_uint64(operation.get("request_id"), "request_id", positive=True),
            require_uint64(operation.get("value"), "value", positive=True),
            str(operation.get("client_host", "")),
            int(operation.get("client_port", 0)),
        )
        if result["classification"] != "NEW" or self.state_version != version:
            raise ProtocolError("replication operation conflicts with local state")

    def to_snapshot(self) -> dict[str, Any]:
        return {
            "num_reqs": self.num_reqs,
            "total_sum": self.total_sum,
            "state_version": self.state_version,
            "election_term": self.election_term,
            "membership_version": self.membership_version,
            "clients": {client_id: asdict(state) for client_id, state in self.clients.items()},
        }

    @classmethod
    def from_snapshot(cls, snapshot: dict[str, Any]) -> "ReplicatedState":
        state = cls(
            num_reqs=require_uint64(snapshot.get("num_reqs"), "num_reqs"),
            total_sum=require_uint64(snapshot.get("total_sum"), "total_sum"),
            state_version=require_uint64(snapshot.get("state_version"), "state_version"),
            election_term=require_uint64(snapshot.get("election_term"), "election_term"),
            membership_version=require_uint64(
                snapshot.get("membership_version"), "membership_version"
            ),
        )
        clients = snapshot.get("clients")
        if not isinstance(clients, dict):
            raise ProtocolError("snapshot clients must be an object")
        for client_id, raw in clients.items():
            if not isinstance(client_id, str) or not isinstance(raw, dict):
                raise ProtocolError("invalid client snapshot")
            state.clients[client_id] = ClientState(
                last_req=require_uint64(raw.get("last_req"), "last_req"),
                last_num_reqs=require_uint64(raw.get("last_num_reqs"), "last_num_reqs"),
                last_total_sum=require_uint64(raw.get("last_total_sum"), "last_total_sum"),
                last_value=require_uint64(raw.get("last_value", 0), "last_value"),
                last_host=str(raw.get("last_host", "")),
                last_port=int(raw.get("last_port", 0)),
            )
        return state


class ServerState:
    def __init__(self, server_id: int, bind_host: str, port: int) -> None:
        self.server_id = server_id
        self.bind_host = bind_host
        self.port = port
        self.role = "JOINING"
        self.leader_id: int | None = None
        self.replicated = ReplicatedState()
        self.members: dict[int, MemberState] = {}
        self.lock = threading.RLock()
        self.commit_lock = threading.Lock()
        self.transitioning = threading.Event()
        self.transitioning.set()

    @property
    def address(self) -> tuple[str, int]:
        return self.bind_host, self.port

    def active_members(self) -> list[MemberState]:
        with self.lock:
            return [
                member
                for member in self.members.values()
                if member.status == "ACTIVE" and member.server_id != self.server_id
            ]

    def membership_payload(self) -> list[dict[str, Any]]:
        with self.lock:
            return [
                {
                    "server_id": member.server_id,
                    "host": member.host,
                    "port": member.port,
                    "status": member.status,
                }
                for member in sorted(self.members.values(), key=lambda item: item.server_id)
            ]

    def install_membership(self, raw_members: list[dict[str, Any]], now: float) -> None:
        members: dict[int, MemberState] = {}
        for raw in raw_members:
            server_id = require_uint64(raw.get("server_id"), "server_id", positive=True)
            members[server_id] = MemberState(
                server_id=server_id,
                host=str(raw["host"]),
                port=int(raw["port"]),
                status=str(raw["status"]),
                last_seen=now,
            )
        with self.lock:
            self.members = members

    def install_snapshot(self, snapshot: dict[str, Any]) -> None:
        replicated = ReplicatedState.from_snapshot(snapshot)
        with self.lock:
            if replicated.state_version < self.replicated.state_version:
                raise ProtocolError("refusing stale snapshot")
            self.replicated = replicated
