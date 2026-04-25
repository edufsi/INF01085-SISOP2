from dataclasses import dataclass, field


@dataclass
class ClientState:
    last_req: int = 0
    last_num_reqs: int = 0
    last_total_sum: int = 0


@dataclass
class ServerState:
    num_requisicoes_total: int = 0
    acumulador_global: int = 0

    # Indexamos por string (apenas o IP) porque a especificação descreve o cliente pela estação/endereço IP.
    # A porta UDP continua disponível em cada recvfrom para responder ao pacote atual, mas não define a identidade lógica do cliente.
    estado_clientes: dict[str, ClientState] = field(default_factory=dict)
