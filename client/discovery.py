import socket
import time
from typing import Any

from common.protocol import ProtocolError, decode, encode, message


def descobrir_servidor(
    cliente: socket.socket,
    porta_destino: int,
    client_id: str,
    timeout: float = 1.0,
    discovery_address: tuple[str, int] | None = None,
) -> tuple[tuple[str, int], dict[str, Any]]:
    deadline = time.monotonic() + timeout
    pedido = encode(message("CLIENT_DISCOVERY", client_id=client_id))
    cliente.sendto(
        pedido,
        discovery_address or ("255.255.255.255", porta_destino),
    )
    while True:
        restante = deadline - time.monotonic()
        if restante <= 0:
            raise socket.timeout
        cliente.settimeout(restante)
        dados, endereco = cliente.recvfrom(2048)
        try:
            resposta = decode(dados)
        except ProtocolError:
            continue
        if resposta["type"] == "LEADER":
            host = resposta.get("host")
            port = resposta.get("port")
            return (
                (str(host), int(port)) if host and port else endereco,
                resposta,
            )
