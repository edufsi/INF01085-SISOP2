import socket
from datetime import datetime

from state import ServerState


def handle_descoberta(socket_servidor: socket.socket, endereco_cliente: tuple[str, int], state: ServerState) -> None:
    if endereco_cliente not in state.estado_clientes:
        state.estado_clientes[endereco_cliente] = 1

    # Especificação não diz nada sobre o que responder para o cliente, 
    # então respondendo aqui com uma mensagem simples
    socket_servidor.sendto("IP_SERVIDOR_OK".encode("utf-8"), endereco_cliente)


def handle_processamento(
    socket_servidor: socket.socket,
    endereco_cliente: tuple[str, int],
    mensagem: str,
    state: ServerState,
) -> None:
    try:
        id_recebido, valor_soma = map(int, mensagem.split(","))

        # estado_clientes: dict[tuple[str, int], int]
        # Mapeia o endereço do cliente (IP, porta) para o próximo ID de requisição esperado desse cliente.
        # Ainda tá faltando aqui guardar também o valor da última soma processada
        id_esperado = state.estado_clientes.get(endereco_cliente, 1)

        # Tudo certo
        if id_recebido == id_esperado:
            state.num_requisicoes_total += 1
            state.acumulador_global += valor_soma
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"{timestamp} client {endereco_cliente[0]} id_req {id_recebido} "
                f"value {valor_soma} num_reqs {state.num_requisicoes_total} total_sum {state.acumulador_global}"
            )
            state.estado_clientes[endereco_cliente] = id_esperado + 1
            ultimo_processado = id_recebido
        
        # Requisição duplicada (provavelmente um ACK perdido)
        elif id_recebido < id_esperado:
            print(f"[Soma] Cliente {endereco_cliente} | ID {id_recebido} duplicado. Ignorando soma.")

            # O que eu faço agora? Tenho que repsonder o ACK com o ID do último pacote que processei com sucesso?
            ultimo_processado = id_recebido
        
        # Requisição fora de ordem (perdido algum pacote anterior)
        else:
            print(f"[Soma] Cliente {endereco_cliente} | ID {id_recebido} fora de ordem. Esperava {id_esperado}.")

            # E se for o primeiro pacote que recebo desse cliente e ele já me mandar um ID 2, por exemplo?
            # Eu estaria respondendo ACK 0, o que não sei se faz sentido
            ultimo_processado = id_esperado - 1

        socket_servidor.sendto(
            f"ACK,{ultimo_processado},{state.num_requisicoes_total},{state.acumulador_global}".encode("utf-8"),
            endereco_cliente,
        )
    except ValueError:
        print(f"[Erro] Mensagem mal formatada de {endereco_cliente}: {mensagem}")

