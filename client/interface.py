import socket
import sys
from datetime import datetime

from discovery import descobrir_servidor
from processing import enviar_valor_stop_and_wait


def iniciar_cliente(porta_destino: int) -> None:
    cliente = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    cliente.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    try:
        # Não estamos calculando o RTT por ping para definir o timeout, mas é o que devemos fazer no futuro
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

    cliente.settimeout(0.01)
    id_requisicao = 1

    # Deveria ter uma thread apenas para escrever as mensagens, e uma thread apenas para ler 
    # os dados do teclado e enviar para o servidor

    while True:
        try:
            entrada = input()
            valor_soma = int(entrada)
            enviar_valor_stop_and_wait(cliente, ip_servidor, porta_destino, id_requisicao, valor_soma)
            id_requisicao += 1
        except (KeyboardInterrupt, EOFError):
            break
        except ValueError:
            continue

    cliente.close()