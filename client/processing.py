from datetime import datetime
from queue import Queue
import socket


def enviar_valor_stop_and_wait(
    cliente: socket.socket,
    ip_servidor: str,
    porta_destino: int,
    id_requisicao: int,
    valor_soma: int,
    output_queue: Queue,
) -> tuple[int, int]:

    mensagem_envio = f"{id_requisicao},{valor_soma}"
    tentativas = 0

    while True:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tipo_envio = "SEND" if tentativas == 0 else "RESEND"
        output_queue.put(f"{timestamp} server {ip_servidor} {tipo_envio} id_req {id_requisicao} value {valor_soma}")
        cliente.sendto(mensagem_envio.encode("utf-8"), (ip_servidor, porta_destino))
        tentativas += 1

        try:
            dados_ack, _ = cliente.recvfrom(1024)
            msg_ack = dados_ack.decode("utf-8")

            # Padrão da mensagem (ACK,ID_REQUISICAO,NUM_REQS,TOTAL_SUM) tirado do absoluto nada mesmo
            # A especificação não diz nada sobre os formatos das mensagens 
            if msg_ack.startswith("ACK,"):
                partes = msg_ack.split(",")
                id_ack = int(partes[1])
                if id_ack == id_requisicao:
                    num_reqs = int(partes[2])
                    total_sum = int(partes[3])
                    return num_reqs, total_sum
        except socket.timeout:
            pass
