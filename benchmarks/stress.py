import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import socket
import sys
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.protocol import decode, encode, message


def discover(port: int) -> tuple[str, int]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(2.0)
    sock.sendto(
        encode(message("CLIENT_DISCOVERY", client_id=uuid.uuid4().hex)),
        ("255.255.255.255", port),
    )
    try:
        while True:
            data, address = sock.recvfrom(2048)
            if decode(data)["type"] == "LEADER":
                return address
    finally:
        sock.close()


def run_client(address: tuple[str, int], requests: int, value: int) -> int:
    client_id = uuid.uuid4().hex
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.5)
    completed = 0
    try:
        for request_id in range(1, requests + 1):
            payload = encode(
                message(
                    "CLIENT_REQUEST",
                    client_id=client_id,
                    request_id=request_id,
                    value=value,
                )
            )
            while True:
                sock.sendto(payload, address)
                try:
                    response = decode(sock.recvfrom(2048)[0])
                except socket.timeout:
                    continue
                if (
                    response["type"] == "CLIENT_ACK"
                    and response.get("request_id") == request_id
                ):
                    completed += 1
                    break
    finally:
        sock.close()
    return completed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("port", type=int)
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--value", type=int, default=1)
    args = parser.parse_args()
    address = discover(args.port)
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.clients) as executor:
        completed = sum(
            executor.map(
                lambda _: run_client(address, args.requests, args.value),
                range(args.clients),
            )
        )
    elapsed = time.monotonic() - started
    print(
        f"completed={completed} elapsed={elapsed:.3f}s "
        f"throughput={completed / elapsed:.1f} req/s"
    )


if __name__ == "__main__":
    main()
