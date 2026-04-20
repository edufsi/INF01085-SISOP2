import socket


def enviar_valor_stop_and_wait(
    cliente: socket.socket,
    ip_servidor: str,
    porta_destino: int,
    id_requisicao: int,
    valor_soma: int,
) -> None:
    mensagem_envio = f"{id_requisicao},{valor_soma}"
    reconhecido = False
    tentativas = 0

    while not reconhecido:
        cliente.sendto(mensagem_envio.encode("utf-8"), (ip_servidor, porta_destino))
        tentativas += 1

        try:
            dados_ack, _ = cliente.recvfrom(1024)
            msg_ack = dados_ack.decode("utf-8")

            if msg_ack.startswith("ACK,"):
                id_ack = int(msg_ack.split(",")[1])
                if id_ack == id_requisicao:
                    print(f"  -> Sucesso! Servidor confirmou o recebimento (ACK {id_ack}).")
                    reconhecido = True
        except socket.timeout:
            print(f"  -> [Timeout] Pacote perdido ou demorou. Reenviando... (Tentativa {tentativas + 1})")

