from datetime import datetime
from queue import Queue
import socket
import time
from typing import Any, Callable

from common.protocol import ProtocolError, decode, encode, message
try:
    from .network_errors import is_transient_network_error
except ImportError:
    from network_errors import is_transient_network_error


LeaderAddress = tuple[str, int]


def enviar_valor_stop_and_wait(
    cliente: socket.socket,
    leader: LeaderAddress,
    client_id: str,
    id_requisicao: int,
    valor_soma: int,
    output_queue: Queue,
    rediscover: Callable[[], LeaderAddress],
    timeout: float = 0.25,
) -> tuple[int, int, LeaderAddress]:
    payload = encode(
        message(
            "CLIENT_REQUEST",
            client_id=client_id,
            request_id=id_requisicao,
            value=valor_soma,
        )
    )
    tentativas = 0
    timeouts = 0
    retries = 0
    while True:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tipo_envio = "SEND" if tentativas == 0 else "RESEND"
        output_queue.put(
            f"{timestamp} server {leader[0]} {tipo_envio} "
            f"id_req {id_requisicao} value {valor_soma}"
        )
        try:
            cliente.sendto(payload, leader)
        except OSError as exc:
            if not is_transient_network_error(exc):
                raise
            leader = rediscover()
            timeouts = 0
            continue
        tentativas += 1
        deadline = time.monotonic() + timeout
        while True:
            restante = deadline - time.monotonic()
            if restante <= 0:
                timeouts += 1
                if timeouts >= 3:
                    leader = rediscover()
                    timeouts = 0
                break
            cliente.settimeout(restante)
            try:
                dados, endereco = cliente.recvfrom(2048)
            except socket.timeout:
                timeouts += 1
                if timeouts >= 3:
                    leader = rediscover()
                    timeouts = 0
                break
            try:
                resposta = decode(dados)
            except ProtocolError:
                continue
            tipo = resposta["type"]
            if tipo == "LEADER":
                leader = endereco
                continue
            if tipo == "NOT_LEADER":
                host = resposta.get("leader_host")
                port = resposta.get("leader_port")
                leader = (str(host), int(port)) if host and port else rediscover()
                retries = 0
                break
            if tipo == "RETRY":
                retries += 1
                if retries >= 3:
                    leader = rediscover()
                    retries = 0
                time.sleep(0.05)
                break
            if tipo == "ERROR":
                raise RuntimeError(str(resposta.get("reason", "server rejected request")))
            if tipo != "CLIENT_ACK":
                continue
            if resposta.get("client_id") != client_id:
                continue
            ack_id = int(resposta["request_id"])
            if ack_id != id_requisicao:
                continue
            retries = 0
            return int(resposta["num_reqs"]), int(resposta["total_sum"]), endereco
