import re
import socket
import subprocess
import sys
from datetime import datetime

from discovery import descobrir_servidor
from processing import enviar_valor_stop_and_wait


def calcular_timeout_por_ping(ip_servidor: str, timeout_padrao: float = 0.01) -> float:
    try:
        resultado_ping = subprocess.run(
            ["ping", "-c", "3", "-W", "1", ip_servidor],
            capture_output=True,
            check=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return timeout_padrao

    correspondencia = re.search(
        r"rtt min/avg/max/(?:mdev|stddev) = [0-9.]+/([0-9.]+)/[0-9.]+/[0-9.]+ ms",
        resultado_ping.stdout,
    )

    if correspondencia is None:
        return timeout_padrao

    rtt_medio_ms = float(correspondencia.group(1))
    return (3 * rtt_medio_ms) / 1000


def configurar_cliente(porta_destino: int) -> tuple[socket.socket, str]:
    cliente = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    cliente.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    try:
        ip_servidor = descobrir_servidor(cliente, porta_destino)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{timestamp} server_addr {ip_servidor}")
    except socket.timeout:
        # A ESPECIFICAÇÃO NÃO DIZ QUE MENSAGEM EXIBIR CASO O SERVIDOR NÃO RESPONDA À DESCOBERTA
        # ELA PARECE INCOMPLETA:
        # "Caso o cliente não receba uma resposta a uma requisição e estoure o tempo de timeout..." e então nada 
        print("[Erro] Nenhum servidor respondeu à descoberta. Ele está ligado?")
        cliente.close()
        sys.exit(1)
    except RuntimeError as erro:
        print(f"[Erro] {erro}")
        cliente.close()
        sys.exit(1)

    cliente.settimeout(calcular_timeout_por_ping(ip_servidor))
    return cliente, ip_servidor


def executar_loop_principal(cliente: socket.socket, ip_servidor: str, porta_destino: int) -> None:
    id_requisicao = 1

    # Deveria ter uma thread apenas para escrever as mensagens, e uma thread apenas para ler 
    # os dados do teclado e enviar para o servidor

    while True:
        try:
            entrada = input()
            valor_soma = int(entrada)
            num_reqs, total_sum = enviar_valor_stop_and_wait(cliente, ip_servidor, porta_destino, id_requisicao, valor_soma)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"{timestamp} server {ip_servidor} id_req {id_requisicao} "
                f"value {valor_soma} num_reqs {num_reqs} total_sum {total_sum}"
            )
            id_requisicao += 1
        except (KeyboardInterrupt, EOFError):
            break
        except ValueError:
            continue


def iniciar_cliente(porta_destino: int) -> None:
    cliente, ip_servidor = configurar_cliente(porta_destino)

    try:
        executar_loop_principal(cliente, ip_servidor, porta_destino)
    finally:
        cliente.close()
