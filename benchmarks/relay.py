from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import socket
import sys
import time
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.protocol import ProtocolError, decode, encode, message


@dataclass
class Registration:
    address: tuple[str, int]
    last_seen: float
    server_id: int


class DiscoveryRelay:
    def __init__(self, bind: tuple[str, int], expiry: float = 5.0) -> None:
        self.bind = bind
        self.expiry = expiry
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(bind)
        self.socket.settimeout(0.2)
        self.servers: dict[int, Registration] = {}
        self.clients: dict[str, tuple[tuple[str, int], float]] = {}
        self.running = True

    def log(self, event: str, **fields: Any) -> None:
        print(
            f"{datetime.now(timezone.utc).isoformat()} relay {event} {fields}",
            flush=True,
        )

    def send(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        try:
            self.socket.sendto(encode(payload), address)
        except (OSError, ProtocolError):
            pass

    def active_servers(self) -> list[Registration]:
        cutoff = time.monotonic() - self.expiry
        self.servers = {
            server_id: registration
            for server_id, registration in self.servers.items()
            if registration.last_seen >= cutoff
        }
        return list(self.servers.values())

    def handle(self, payload: dict[str, Any], address: tuple[str, int]) -> None:
        message_type = payload["type"]
        if message_type == "RELAY_REGISTER":
            server_id = int(payload["server_id"])
            endpoint = (str(payload["host"]), int(payload["port"]))
            is_new = server_id not in self.servers
            self.servers[server_id] = Registration(
                endpoint, time.monotonic(), server_id
            )
            if is_new:
                self.log("registered", server_id=server_id, endpoint=endpoint)
            return
        if message_type == "CLIENT_DISCOVERY":
            client_id = str(payload["client_id"])
            self.clients[client_id] = (address, time.monotonic())
            for registration in self.active_servers():
                self.send(payload, registration.address)
            return
        if message_type == "LEADER" and payload.get("client_id") is not None:
            client = self.clients.get(str(payload["client_id"]))
            if client is not None:
                self.send(payload, client[0])
            return
        if message_type == "RELAY_STATUS_REQUEST":
            self.send(
                message(
                    "RELAY_STATUS_RESPONSE",
                    request_id=payload.get("request_id"),
                    server_ids=sorted(
                        registration.server_id
                        for registration in self.active_servers()
                    ),
                ),
                address,
            )
            return
        if message_type.startswith("SERVER_") or message_type in {
            "HEARTBEAT",
            "BACKUP_HEARTBEAT",
            "MEMBERSHIP",
            "COORDINATOR",
            "LEAVE",
        }:
            for registration in self.active_servers():
                self.send(payload, registration.address)

    def serve_forever(self) -> None:
        self.log("started", bind=self.bind)
        while self.running:
            try:
                data, address = self.socket.recvfrom(2048)
            except socket.timeout:
                self.active_servers()
                continue
            except OSError:
                break
            try:
                self.handle(decode(data), address)
            except (ProtocolError, KeyError, TypeError, ValueError) as exc:
                self.log("invalid", source=address, reason=str(exc))


def parse_endpoint(value: str) -> tuple[str, int]:
    host, separator, raw_port = value.rpartition(":")
    if not separator:
        raise argparse.ArgumentTypeError("use HOST:PORT")
    return host, int(raw_port)


def main() -> None:
    parser = argparse.ArgumentParser(description="UDP discovery relay")
    parser.add_argument("--bind", type=parse_endpoint, required=True)
    parser.add_argument("--expiry", type=float, default=5.0)
    args = parser.parse_args()
    relay = DiscoveryRelay(args.bind, args.expiry)
    try:
        relay.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        relay.socket.close()


if __name__ == "__main__":
    main()
