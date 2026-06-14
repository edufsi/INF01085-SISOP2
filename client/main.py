import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from interface import iniciar_cliente


def parse_endpoint(value: str) -> tuple[str, int]:
    host, separator, raw_port = value.rpartition(":")
    if not separator or not host:
        raise argparse.ArgumentTypeError("use HOST:PORT")
    return host, int(raw_port)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cliente do serviço distribuído de soma")
    parser.add_argument("cluster_port", type=int)
    parser.add_argument("--bind", default="", help="Endereço IPv4 local")
    parser.add_argument("--discovery", type=parse_endpoint, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not 1 <= args.cluster_port <= 65535:
        raise SystemExit("A porta deve estar entre 1 e 65535.")
    iniciar_cliente(args.cluster_port, args.bind, args.discovery)


if __name__ == "__main__":
    main()
