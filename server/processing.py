import socket
from datetime import datetime

from state import ClientState, ServerState


def handle_descoberta(socket_servidor: socket.socket, endereco_cliente: tuple[str, int], state: ServerState) -> None:
    ip_cliente, porta_cliente = endereco_cliente

    if endereco_cliente not in state.estado_clientes:
        state.estado_clientes[endereco_cliente] = ClientState()

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
        ip_cliente, porta_cliente = endereco_cliente

        # VERIFICA SE O CLIENTE É NOVO ANTES DE INSERIR NO ESTADO
        cliente_eh_novo = endereco_cliente not in state.estado_clientes

        # SE FOR NOVO E O ID > 1, ASSUME QUE O SERVIDOR REINICIOU!!!
        if cliente_eh_novo and id_recebido > 1:
            print(f"[Aviso] Cliente {ip_cliente} enviou ID {id_recebido}, mas não o conhecemos (Servidor reiniciou?). Enviando RESET.")
            socket_servidor.sendto("RESET".encode("utf-8"), endereco_cliente)
            return  # não manda ACK.

        # Se passou pelo if acima, ou é cliente conhecido ou é cliente novo mandando ID 1 (o que é normal)
        client_state = state.estado_clientes.setdefault(endereco_cliente, ClientState())
        id_esperado = client_state.last_req + 1

        # Tudo certo
        if id_recebido == id_esperado:
            state.num_requisicoes_total += 1
            state.acumulador_global += valor_soma
            client_state.last_req = id_recebido
            client_state.last_num_reqs = state.num_requisicoes_total
            client_state.last_total_sum = state.acumulador_global
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"{timestamp} client {ip_cliente} id_req {id_recebido} "
                f"value {valor_soma} num_reqs {client_state.last_num_reqs} total_sum {client_state.last_total_sum}"
            )
        
        # Requisição duplicada (provavelmente um ACK perdido)
        elif id_recebido < id_esperado:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"{timestamp} client {ip_cliente} DUP!! id_req {client_state.last_req} "
                f"value {valor_soma} num_reqs {client_state.last_num_reqs} total_sum {client_state.last_total_sum}"
            )
        
        # Requisição fora de ordem, maior que o esperado (isso não deveria acontecer com um servidor que não reiniciou, 
        # porque o cliente só manda requisições na ordem dos acks recebidos, mas VAI QUE né )
        else:
            print(f"[Soma] Cliente {ip_cliente} | ID {id_recebido} fora de ordem. Esperava {id_esperado}.")

        socket_servidor.sendto(
            f"ACK,{client_state.last_req},{client_state.last_num_reqs},{client_state.last_total_sum}".encode("utf-8"),
            endereco_cliente,
        )
    except ValueError:
        print(f"[Erro] Mensagem mal formatada de {endereco_cliente}: {mensagem}")