import re
import select
import socket
import subprocess
import sys
import threading
from datetime import datetime
from queue import Empty, Queue

from discovery import descobrir_servidor
from processing import enviar_valor_stop_and_wait


def ler_entrada_usuario(input_queue: Queue) -> None:
    while True:
        linha = sys.stdin.readline()
        
        if linha == "":
            input_queue.put(None)
            break

        try:
            # O strip() remove o '\n' do final antes de converter
            valor_soma = int(linha.strip())
            input_queue.put(valor_soma)
        except ValueError:
            # Se não for um inteiro válido (ex: letras), apenas ignora 
            # silenciosamente e volta a aguardar a próxima linha
            continue


def escrever_mensagens(output_queue: Queue) -> None:
    while True:
        mensagem = output_queue.get()
        if mensagem is None:
            break
        print(mensagem)


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

    timeout_calculado = (3 * rtt_medio_ms) / 1000
    return max(timeout_calculado, 0.01)


def configurar_cliente(porta_destino: int, output_queue: Queue) -> tuple[socket.socket, str]:
    cliente = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    cliente.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    try:
        ip_servidor = descobrir_servidor(cliente, porta_destino)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        output_queue.put(f"{timestamp} server_addr {ip_servidor}")
    except socket.timeout:
        # A ESPECIFICAÇÃO NÃO DIZ QUE MENSAGEM EXIBIR CASO O SERVIDOR NÃO RESPONDA À DESCOBERTA
        # ELA PARECE INCOMPLETA:
        # "Caso o cliente não receba uma resposta a uma requisição e estoure o tempo de timeout..." e então nada 
        output_queue.put("[Erro] Nenhum servidor respondeu à descoberta. Ele está ligado?")
        cliente.close()
        raise SystemExit(1)
    except RuntimeError as erro:
        output_queue.put(f"[Erro] {erro}")
        cliente.close()
        raise SystemExit(1)

    cliente.settimeout(calcular_timeout_por_ping(ip_servidor))
    return cliente, ip_servidor


def executar_loop_principal(
    cliente: socket.socket,
    ip_servidor: str,
    porta_destino: int,
    input_queue: Queue,
    output_queue: Queue,
) -> None:
    id_requisicao = 1

    try:
        while True:

            valor_soma = input_queue.get()

            if valor_soma is None:
                break
            
            while True:
                num_reqs, total_sum = enviar_valor_stop_and_wait(
                    cliente,
                    ip_servidor,
                    porta_destino,
                    id_requisicao,
                    valor_soma,
                    output_queue,
                )


                # Quando a função retorna (-1, -1) é porque recebeu um RESET do servidor, ou seja, o servidor reiniciou e esqueceu o estado de todos os clientes
                # Neste caso, o cliente precisa resetar seu ID para 1 e tentar enviar a mesma requisição de novo (com o mesmo valor_soma, mas id_requisicao = 1)
                if num_reqs == -1 and total_sum == -1:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    output_queue.put(f"{timestamp} [Aviso] Servidor reiniciou! Resetando ID para 1...")
                    id_requisicao = 1
                                        
                    continue
                
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                output_queue.put(
                    f"{timestamp} server {ip_servidor} id_req {id_requisicao} "
                    f"value {valor_soma} num_reqs {num_reqs} total_sum {total_sum}"
                )
                id_requisicao += 1
                
                break 
                
    except KeyboardInterrupt:
        print("\n[!] Interrupção de teclado detectada.")






def iniciar_cliente(porta_destino: int) -> None:
    output_queue = Queue()
    thread_escrita = threading.Thread(target=escrever_mensagens, args=(output_queue,))
    thread_escrita.start()

    cliente = None

    try:
        cliente, ip_servidor = configurar_cliente(porta_destino, output_queue)

        input_queue = Queue(maxsize=1)
        thread_entrada = threading.Thread(target=ler_entrada_usuario, args=(input_queue,), daemon=True)
        thread_entrada.start()

        executar_loop_principal(cliente, ip_servidor, porta_destino, input_queue, output_queue)
    finally:
        if cliente is not None:
            cliente.close()
        output_queue.put(None)
        thread_escrita.join()
