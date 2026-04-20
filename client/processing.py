import socket
from datetime import datetime


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
                partes = msg_ack.split(",")
                id_ack = int(partes[1])
                if id_ack == id_requisicao:
                    num_reqs = int(partes[2])
                    total_sum = int(partes[3])
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(
                        f"{timestamp} server {ip_servidor} id_req {id_requisicao} "
                        f"value {valor_soma} num_reqs {num_reqs} total_sum {total_sum}"
                    )
                    reconhecido = True
        except socket.timeout:
            pass

