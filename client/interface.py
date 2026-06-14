from datetime import datetime
from queue import Queue
import socket
import sys
import threading
import time
import uuid

from common.protocol import MAX_UINT64, encode, message
try:
    from .discovery import descobrir_servidor
    from .processing import LeaderAddress, enviar_valor_stop_and_wait
except ImportError:
    from discovery import descobrir_servidor
    from processing import LeaderAddress, enviar_valor_stop_and_wait


def ler_entrada_usuario(input_queue: Queue) -> None:
    while True:
        linha = sys.stdin.readline()
        if linha == "":
            input_queue.put(None)
            return
        try:
            valor = int(linha.strip())
        except ValueError:
            continue
        if 1 <= valor <= MAX_UINT64:
            input_queue.put(valor)


def escrever_mensagens(output_queue: Queue) -> None:
    while True:
        mensagem = output_queue.get()
        if mensagem is None:
            return
        print(mensagem, flush=True)


def configurar_cliente(
    porta_destino: int,
    bind_host: str,
) -> socket.socket:
    cliente = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    cliente.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    cliente.bind((bind_host, 0))
    return cliente


def descobrir_com_retry(
    cliente: socket.socket,
    porta_destino: int,
    client_id: str,
    output_queue: Queue,
    discovery_address: tuple[str, int] | None = None,
) -> LeaderAddress:
    while True:
        try:
            endereco, _ = descobrir_servidor(
                cliente,
                porta_destino,
                client_id,
                timeout=1.0,
                discovery_address=discovery_address,
            )
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            output_queue.put(f"{timestamp} server_addr {endereco[0]}")
            return endereco
        except socket.timeout:
            output_queue.put("[Aviso] Aguardando eleição de um servidor primário...")
            time.sleep(0.2)


def executar_loop_principal(
    cliente: socket.socket,
    leader: LeaderAddress,
    porta_destino: int,
    client_id: str,
    input_queue: Queue,
    output_queue: Queue,
    discovery_address: tuple[str, int] | None = None,
) -> LeaderAddress:
    id_requisicao = 1

    def rediscover() -> LeaderAddress:
        return descobrir_com_retry(
            cliente,
            porta_destino,
            client_id,
            output_queue,
            discovery_address,
        )

    while True:
        valor_soma = input_queue.get()
        if valor_soma is None:
            return leader
        num_reqs, total_sum, leader = enviar_valor_stop_and_wait(
            cliente,
            leader,
            client_id,
            id_requisicao,
            valor_soma,
            output_queue,
            rediscover,
        )
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        output_queue.put(
            f"{timestamp} server {leader[0]} id_req {id_requisicao} "
            f"value {valor_soma} num_reqs {num_reqs} total_sum {total_sum}"
        )
        id_requisicao += 1


def iniciar_cliente(
    porta_destino: int,
    bind_host: str = "",
    discovery_address: tuple[str, int] | None = None,
) -> None:
    client_id = uuid.uuid4().hex
    output_queue: Queue = Queue()
    thread_escrita = threading.Thread(
        target=escrever_mensagens, args=(output_queue,), daemon=True
    )
    thread_escrita.start()
    cliente = configurar_cliente(porta_destino, bind_host)
    leader: LeaderAddress | None = None
    try:
        leader = descobrir_com_retry(
            cliente,
            porta_destino,
            client_id,
            output_queue,
            discovery_address,
        )
        input_queue: Queue = Queue(maxsize=1)
        threading.Thread(
            target=ler_entrada_usuario, args=(input_queue,), daemon=True
        ).start()
        leader = executar_loop_principal(
            cliente,
            leader,
            porta_destino,
            client_id,
            input_queue,
            output_queue,
            discovery_address,
        )
    except KeyboardInterrupt:
        pass
    finally:
        if leader is not None:
            try:
                cliente.sendto(
                    encode(message("CLIENT_LEAVE", client_id=client_id)), leader
                )
            except OSError:
                pass
        cliente.close()
        output_queue.put(None)
        thread_escrita.join(timeout=1.0)
