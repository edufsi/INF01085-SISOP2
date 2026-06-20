import socket
import time
from typing import Any

from common.protocol import ProtocolError, decode, encode, message


def descobrir_servidor(
    cliente: socket.socket,
    porta_destino: int,
    client_id: str,
    timeout: float = 1.0,
    discovery_address: tuple[str, int] | None = None,
) -> tuple[tuple[str, int], dict[str, Any]]:
    
    # Define uma janela curta de coleta (ex: 0.3 segundos) 
    # para não deixar o cliente travado esperando à toa se o líder responder rápido.
    tempo_coleta = min(0.3, timeout) 
    deadline = time.monotonic() + tempo_coleta
    
    pedido = encode(message("CLIENT_DISCOVERY", client_id=client_id))
    cliente.sendto(
        pedido,
        discovery_address or ("255.255.255.255", porta_destino),
    )
    
    lideres_encontrados = []

    # Fase de Escuta: Coleta todas as respostas até o tempo acabar
    # Estamos fazendo isso para o caso de haver mais de um líder respondendo
    # Por exemplo: um nodo foi desconectado, ficou sozainho e se elegeu líder, depois foi conectado no sistema de volta, 
    # e nesse exato momento tem dois líderes e estamos mandando o discovery. Ambos vão responder, mas só um tem os dados mais atualizados
    while True:
        restante = deadline - time.monotonic()
        if restante <= 0:
            break  # Janela de escuta fechou

        cliente.settimeout(restante)
        
        try:
            dados, endereco = cliente.recvfrom(2048)
            resposta = decode(dados)
            
            if resposta.get("type") == "LEADER":
                # Salva a resposta na lista em vez de retornar imediatamente
                lideres_encontrados.append((endereco, resposta))
                
        except socket.timeout:
            break  # Estourou o timeout do socket, sai do loop
        except ProtocolError:
            continue

    # Verifica se alguém respondeu
    if not lideres_encontrados:
        raise socket.timeout("Nenhum servidor respondeu ao discovery.")

    # Avalia todos os líderes que responderam e escolhe o que 
    # possui a MAIOR state_version (os dados mais atualizados)
    melhor_lider = max(
        lideres_encontrados,
        key=lambda item: (
            int(item[1].get("state_version", 0)),
            int(item[1].get("leader_id", item[1].get("server_id", 0)))
        )
    )

    endereco_vencedor, resposta_vencedora = melhor_lider
    
    host = resposta_vencedora.get("host")
    port = resposta_vencedora.get("port")
    
    endereco_final = (str(host), int(port)) if host and port else endereco_vencedor
    
    return endereco_final, resposta_vencedora