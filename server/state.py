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

    # Indexamos por (IP, porta) porque o recvfrom já nos entrega esse identificador naturalmente
    # e isso permite distinguir múltiplos clientes executando na mesma máquina.
    estado_clientes: dict[tuple[str, int], ClientState] = field(default_factory=dict)
