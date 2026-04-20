import socket


def descobrir_servidor(cliente: socket.socket, porta_destino: int, timeout: float = 5.0) -> str:
    endereco_broadcast = ("<broadcast>", porta_destino)
    cliente.settimeout(timeout)
    cliente.sendto("DESCOBERTA".encode("utf-8"), endereco_broadcast)

    dados, endereco_servidor = cliente.recvfrom(1024)
    mensagem = dados.decode("utf-8")

    if mensagem != "IP_SERVIDOR_OK":
        raise RuntimeError("Resposta desconhecida na descoberta.")

    return endereco_servidor[0]

