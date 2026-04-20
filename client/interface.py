import socket
import sys

from .discovery import descobrir_servidor
from .processing import enviar_valor_stop_and_wait


def iniciar_cliente(porta_destino: int) -> None:
    cliente = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    cliente.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    print("--- Iniciando Cliente ---")
    print(f"Procurando servidor na porta {porta_destino}...\n")

    try:
        ip_servidor = descobrir_servidor(cliente, porta_destino)
        print(f"[Sucesso] Servidor encontrado no IP: {ip_servidor}")
    except socket.timeout:
        print("[Erro] Nenhum servidor respondeu à descoberta. Ele está ligado?")
        cliente.close()
        sys.exit(1)
    except RuntimeError as erro:
        print(f"[Erro] {erro}")
        cliente.close()
        sys.exit(1)

    print("\n--- Fase de Processamento Iniciada ---")
    print("Digite um número para somar ou 'sair' para encerrar.")

    cliente.settimeout(0.01)
    id_requisicao = 1

    while True:
        try:
            entrada = input(f"\n[Req ID: {id_requisicao}] Digite o valor a somar: ")
            if entrada.lower() == "sair":
                break

            valor_soma = int(entrada)
            enviar_valor_stop_and_wait(cliente, ip_servidor, porta_destino, id_requisicao, valor_soma)
            id_requisicao += 1
        except ValueError:
            print("Por favor, digite um número inteiro válido.")

    print("\nEncerrando cliente...")
    cliente.close()

