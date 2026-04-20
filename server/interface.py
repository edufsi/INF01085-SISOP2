import socket
from datetime import datetime

from .processing import handle_descoberta, handle_processamento
from .state import ServerState


def iniciar_servidor(porta: int) -> None:


    """
    Cria o socket do servidor.

    socket.AF_INET: Define a Família de Endereços (Address Family). 
    O AF_INET diz que vamos usar endereços IPv4. 
    Se fosse IPv6, usaria AF_INET6
    
    socket.SOCK_DGRAM: Define o Tipo de Socket. SOCK_DGRAM significa Datagrama. 
    Na prática, significa "Quero usar o protocolo UDP". 
    (Se fôssemos usar o protocolo TCP seria socket.SOCK_STREAM).
    """
    servidor = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


    """
    Permite reutilizar o endereço.

    Quando para o seu servidor (apertando Ctrl+C, por exemplo) e tenta rodar o script de novo logo em seguida, 
    o sistema operacional às vezes mantém a porta "presa" por alguns segundos achando que ainda há pacotes 
    perdidos a caminho.

    setsockopt: Significa "Set Socket Options" (Configurar Opções do Socket).
    socket.SOL_SOCKET: Diz que a configuração que vamos fazer é a nível geral do socket.
    socket.SO_REUSEADDR: Essa é a regra em si. Significa "Socket Option: Reuse Address" 
    (Permitir o reúso imediato do endereço e porta).
    1: É o valor booleano True. Estamos ativando essa opção. Com isso, mesmo que a porta 
    não tenha sido liberada 100% pelo Windows/Linux, o seu script pode "roubá-la" de volta 
    imediatamente ao reiniciar.
    """
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind(("", porta))

    state = ServerState()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} num_reqs {state.num_requisicoes_total} total_sum {state.acumulador_global}")

    while True:
        dados, endereco_cliente = servidor.recvfrom(1024)
        mensagem = dados.decode("utf-8")

        if mensagem == "DESCOBERTA":
            handle_descoberta(servidor, endereco_cliente, state)
        else:
            handle_processamento(servidor, endereco_cliente, mensagem, state)

