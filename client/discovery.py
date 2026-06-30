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
    collection_window = min(0.3, timeout)
    deadline = time.monotonic() + collection_window

    pedido = encode(message("CLIENT_DISCOVERY", client_id=client_id))
    cliente.sendto(
        pedido,
        discovery_address or ("255.255.255.255", porta_destino),
    )

    lideres_encontrados = []

    # Collect briefly in case a partition left more than one leader responding.
    while True:
        restante = deadline - time.monotonic()
        if restante <= 0:
            break

        cliente.settimeout(restante)

        try:
            dados, endereco = cliente.recvfrom(2048)
            resposta = decode(dados)

            if resposta.get("type") == "LEADER":
                lideres_encontrados.append((endereco, resposta))

        except socket.timeout:
            break
        except ProtocolError:
            continue

    if not lideres_encontrados:
        raise socket.timeout("Nenhum servidor respondeu ao discovery.")

    melhor_lider = max(
        lideres_encontrados,
        key=lambda item: (
            int(item[1].get("state_version", 0)),
            int(item[1].get("leader_id", item[1].get("server_id", 0))),
        ),
    )

    endereco_vencedor, resposta_vencedora = melhor_lider

    host = resposta_vencedora.get("host")
    port = resposta_vencedora.get("port")

    if discovery_address is None:
        endereco_final = (
            endereco_vencedor[0],
            int(port) if port else endereco_vencedor[1],
        )
    else:
        endereco_final = (str(host), int(port)) if host and port else endereco_vencedor

    return endereco_final, resposta_vencedora
