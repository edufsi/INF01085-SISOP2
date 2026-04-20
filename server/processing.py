import socket

from .state import ServerState


def handle_descoberta(socket_servidor: socket.socket, endereco_cliente: tuple[str, int], state: ServerState) -> None:
    print(f"[Descoberta] Cliente {endereco_cliente} encontrou o servidor.")

    if endereco_cliente not in state.estado_clientes:
        state.estado_clientes[endereco_cliente] = 1

    socket_servidor.sendto("IP_SERVIDOR_OK".encode("utf-8"), endereco_cliente)


def handle_processamento(
    socket_servidor: socket.socket,
    endereco_cliente: tuple[str, int],
    mensagem: str,
    state: ServerState,
) -> None:
    try:
        id_recebido, valor_soma = map(int, mensagem.split(","))
        id_esperado = state.estado_clientes.get(endereco_cliente, 1)

        if id_recebido == id_esperado:
            state.acumulador_global += valor_soma
            print(
                f"[Soma] Cliente {endereco_cliente} | ID {id_recebido} processado. "
                f"Valor: {valor_soma} | Novo Total Global: {state.acumulador_global}"
            )
            state.estado_clientes[endereco_cliente] = id_esperado + 1
            ultimo_processado = id_recebido
        elif id_recebido < id_esperado:
            print(f"[Soma] Cliente {endereco_cliente} | ID {id_recebido} duplicado. Ignorando soma.")

            # O que eu faço agora? Tenho que repsonder o ACK com o ID do último pacote que processei com sucesso?
            ultimo_processado = id_recebido
        else:
            print(f"[Soma] Cliente {endereco_cliente} | ID {id_recebido} fora de ordem. Esperava {id_esperado}.")

            # E se for o primeiro pacote que recebo desse cliente e ele já me mandar um ID 2, por exemplo?
            # Eu estaria respondendo ACK 0, o que não sei se faz sentido
            ultimo_processado = id_esperado - 1

        socket_servidor.sendto(f"ACK,{ultimo_processado}".encode("utf-8"), endereco_cliente)
    except ValueError:
        print(f"[Erro] Mensagem mal formatada de {endereco_cliente}: {mensagem}")

