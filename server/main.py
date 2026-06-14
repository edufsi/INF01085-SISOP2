import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from interface import iniciar_servidor
from node import random_server_id


def parse_endpoint(value: str) -> tuple[str, int]:
    host, separator, raw_port = value.rpartition(":")
    if not separator or not host:
        raise argparse.ArgumentTypeError("use HOST:PORT")
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return host, port


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replica manager do serviço de soma")
    parser.add_argument("cluster_port", type=int)
    parser.add_argument("--server-id", type=int, default=None)
    parser.add_argument("--bind", default="", help="Endereço IPv4 local")
    parser.add_argument("--discovery", type=parse_endpoint, default=None)
    parser.add_argument("--quiet-requests", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not 1 <= args.cluster_port <= 65535:
        raise SystemExit("A porta deve estar entre 1 e 65535.")
    server_id = args.server_id if args.server_id is not None else random_server_id()
    if not 1 <= server_id <= (1 << 64) - 1:
        raise SystemExit("O server-id deve ser um unsigned int de 64 bits positivo.")
    iniciar_servidor(
        args.cluster_port,
        server_id,
        args.bind,
        args.discovery,
        request_logging=not args.quiet_requests,
    )


if __name__ == "__main__":
    main()
