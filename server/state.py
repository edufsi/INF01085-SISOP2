from dataclasses import dataclass, field


@dataclass
class ServerState:
    num_requisicoes_total: int = 0
    acumulador_global: int = 0
    estado_clientes: dict[tuple[str, int], int] = field(default_factory=dict)
