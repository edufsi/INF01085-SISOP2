from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import random
import signal
import socket
import sys
import time
import uuid
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.protocol import ProtocolError, decode, encode, message


def parse_endpoint(value: str) -> tuple[str, int]:
    host, separator, raw_port = value.rpartition(":")
    if not separator or not host:
        raise argparse.ArgumentTypeError("use HOST:PORT")
    return host, int(raw_port)


class WorkloadClient:
    def __init__(
        self,
        seed: int,
        client_index: int,
        count: int,
        bind: tuple[str, int],
        discovery: tuple[str, int],
        *,
        client_id: str | None = None,
        request_id: int = 1,
        position: int = 0,
        pending_value: int | None = None,
        startup_hold: float = 0.0,
    ) -> None:
        rng = random.Random(seed + client_index)
        self.values = [rng.randint(1, 100) for _ in range(count)]
        pending = None
        if pending_value is not None:
            pending = {
                "client_id": client_id,
                "request_id": request_id,
                "value": pending_value,
                "position": position,
            }
        self.state = {
            "client_id": client_id or uuid.uuid4().hex,
            "request_id": request_id,
            "position": position,
            "pending": pending,
            "completed": position >= count,
            "error": None,
        }
        if pending is not None:
            pending["client_id"] = self.state["client_id"]
        self.bind = bind
        self.discovery = discovery
        self.startup_hold = startup_hold
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(bind)
        self.socket.settimeout(0.1)
        self.stop_requested = False
        self.running = True
        self.leader: tuple[str, int] | None = None

    def log(self, event: str, **fields: Any) -> None:
        print(
            json.dumps(
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "event": event,
                    "pid": os.getpid(),
                    **fields,
                },
                sort_keys=True,
            ),
            flush=True,
        )

    def request_stop(self, _signum: int, _frame: Any) -> None:
        self.stop_requested = True
        self.log("sigterm_received", pending=self.state.get("pending"))

    def status(self, request_id: Any) -> dict[str, Any]:
        return message(
            "STATUS_RESPONSE",
            request_id=request_id,
            component="client",
            pid=os.getpid(),
            client_id=self.state["client_id"],
            next_request_id=self.state["request_id"],
            position=self.state["position"],
            total=len(self.values),
            pending=self.state["pending"],
            completed=self.state["completed"],
            error=self.state["error"],
        )

    def handle_control(self, payload: dict[str, Any], address: tuple[str, int]) -> bool:
        if payload["type"] != "STATUS_REQUEST":
            return False
        self.socket.sendto(encode(self.status(payload.get("request_id"))), address)
        return True

    def receive_until(
        self, deadline: float
    ) -> tuple[dict[str, Any], tuple[str, int]] | None:
        while time.monotonic() < deadline:
            self.socket.settimeout(max(0.001, deadline - time.monotonic()))
            try:
                data, address = self.socket.recvfrom(2048)
            except socket.timeout:
                return None
            payload = decode(data)
            if self.handle_control(payload, address):
                continue
            return payload, address
        return None

    def discover(self) -> tuple[str, int]:
        request = encode(
            message(
                "CLIENT_DISCOVERY",
                client_id=self.state["client_id"],
            )
        )
        while self.running:
            self.socket.sendto(request, self.discovery)
            response = self.receive_until(time.monotonic() + 0.5)
            if response is None:
                continue
            payload, address = response
            if payload["type"] == "LEADER":
                host = payload.get("host")
                port = payload.get("port")
                return (
                    (str(host), int(port))
                    if host and port
                    else address
                )
        raise RuntimeError("client stopped")

    def ensure_pending(self) -> dict[str, Any]:
        pending = self.state.get("pending")
        if pending is None:
            position = int(self.state["position"])
            pending = {
                "client_id": self.state["client_id"],
                "request_id": self.state["request_id"],
                "value": self.values[position],
                "position": position,
            }
            self.state["pending"] = pending
        return pending

    def accept_ack(self, pending: dict[str, Any]) -> None:
        if self.state.get("pending") != pending:
            raise RuntimeError("pending request changed")
        self.state["position"] = int(pending["position"]) + 1
        self.state["request_id"] = int(pending["request_id"]) + 1
        self.state["pending"] = None
        if self.state["position"] >= len(self.values):
            self.state["completed"] = True

    def idle(self) -> None:
        while self.running and not self.stop_requested:
            response = self.receive_until(time.monotonic() + 0.2)
            if response is None:
                continue
        self.running = False

    def hold_before_sending(self) -> None:
        deadline = time.monotonic() + self.startup_hold
        while self.running and not self.stop_requested and time.monotonic() < deadline:
            response = self.receive_until(min(deadline, time.monotonic() + 0.1))
            if response is None:
                continue

    def hold_before_exit(self) -> None:
        deadline = time.monotonic() + 0.5
        while time.monotonic() < deadline:
            response = self.receive_until(min(deadline, time.monotonic() + 0.1))
            if response is None:
                continue

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)
        self.log(
            "started",
            bind=self.bind,
            discovery=self.discovery,
            position=self.state["position"],
            pending=self.state["pending"],
        )
        try:
            if self.state["completed"]:
                self.idle()
                return
            self.hold_before_sending()
            self.leader = self.discover()
            while self.running:
                if self.stop_requested and self.state.get("pending") is None:
                    break
                pending = self.ensure_pending()
                request = encode(
                    message(
                        "CLIENT_REQUEST",
                        client_id=pending["client_id"],
                        request_id=pending["request_id"],
                        value=pending["value"],
                    )
                )
                assert self.leader is not None
                self.socket.sendto(request, self.leader)
                deadline = time.monotonic() + 0.25
                acknowledged = False
                while True:
                    response = self.receive_until(deadline)
                    if response is None:
                        self.leader = self.discover()
                        break
                    payload, address = response
                    message_type = payload["type"]
                    if message_type == "CLIENT_ACK":
                        if (
                            payload.get("client_id") != pending["client_id"]
                            or int(payload["request_id"])
                            != int(pending["request_id"])
                        ):
                            continue
                        self.leader = address
                        self.accept_ack(pending)
                        acknowledged = True
                        break
                    if message_type == "LEADER":
                        self.leader = (
                            str(payload.get("host", address[0])),
                            int(payload.get("port", address[1])),
                        )
                        break
                    if message_type == "NOT_LEADER":
                        host = payload.get("leader_host")
                        port = payload.get("leader_port")
                        self.leader = (
                            (str(host), int(port))
                            if host and port
                            else self.discover()
                        )
                        break
                    if message_type == "RETRY":
                        time.sleep(0.01)
                        break
                    if message_type == "ERROR":
                        raise RuntimeError(str(payload.get("reason")))
                if acknowledged:
                    if self.state["completed"]:
                        self.log("completed", position=self.state["position"])
                        self.idle()
                        return
                    if self.stop_requested:
                        break
            if self.stop_requested:
                self.hold_before_exit()
        except Exception as exc:
            self.state["error"] = repr(exc)
            self.log("error", error=repr(exc))
            raise
        finally:
            if self.stop_requested and self.leader is not None:
                try:
                    self.socket.sendto(
                        encode(
                            message(
                                "CLIENT_LEAVE",
                                client_id=self.state["client_id"],
                            )
                        ),
                        self.leader,
                    )
                except OSError:
                    pass
            self.log(
                "stopped",
                position=self.state["position"],
                pending=self.state["pending"],
            )
            self.socket.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="In-memory benchmark client")
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--client-index", type=int, required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--bind", type=parse_endpoint, required=True)
    parser.add_argument("--discovery", type=parse_endpoint, required=True)
    parser.add_argument("--client-id")
    parser.add_argument("--request-id", type=int, default=1)
    parser.add_argument("--position", type=int, default=0)
    parser.add_argument("--pending-value", type=int)
    parser.add_argument("--startup-hold", type=float, default=0.0)
    args = parser.parse_args()
    client = WorkloadClient(
        args.seed,
        args.client_index,
        args.count,
        args.bind,
        args.discovery,
        client_id=args.client_id,
        request_id=args.request_id,
        position=args.position,
        pending_value=args.pending_value,
        startup_hold=args.startup_hold,
    )
    client.run()


if __name__ == "__main__":
    main()
