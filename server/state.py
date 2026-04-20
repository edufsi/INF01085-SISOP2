from dataclasses import dataclass, field


@dataclass
class ServerState:
    num_requisicoes_total: int = 0
    acumulador_global: int = 0

    # Mapeia o endereço do cliente (IP, porta) para o próximo ID de requisição esperado desse cliente.
    # Ainda tá faltando aqui guardar também o valor da última soma processada
    estado_clientes: dict[tuple[str, int], int] = field(default_factory=dict)

