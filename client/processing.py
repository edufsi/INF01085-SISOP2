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
            # LOOP INTERNO: Fica secando o buffer até achar o ACK certo ou dar timeout
            while True:
                dados_ack, endereco_ack = cliente.recvfrom(1024)
                
                if endereco_ack != (ip_servidor, porta_destino):
                    continue # Volta para o topo do loop de escuta (não reenvia)

                msg_ack = dados_ack.decode("utf-8")

                if msg_ack == "RESET":
                    return -1, -1  # Ordem de reset: ID volta para 1, acumulador local volta para 0
                
                if msg_ack.startswith("ACK,"):
                    partes = msg_ack.split(",")
                    id_ack = int(partes[1])
                    
                    if id_ack == id_requisicao:
                        # É O PACOTE CERTO!
                        num_reqs = int(partes[2])
                        total_sum = int(partes[3])
                        return num_reqs, total_sum
                    
                    # SE CHEGAR AQUI É PORQUE ERA UM ACK VELHO.
                    # Não preciso re-enviar, só ignoro e continuo esperando o ACK certo ou o timeout.
                    # O loop 'while True' interno dá a volta, lendo o próximo pacote
        except socket.timeout:
            pass
