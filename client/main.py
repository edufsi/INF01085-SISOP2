import sys

from interface import iniciar_cliente


def main() -> None:
    if len(sys.argv) != 2:
        print("Uso incorreto!")
        print("Como usar: python client.py <PORTA_DO_SERVIDOR>")
        print("Exemplo:   python client.py 50000")
        sys.exit(1)

    try:
        porta_escolhida = int(sys.argv[1])
        iniciar_cliente(porta_escolhida)
    except ValueError:
        print("Erro: A porta deve ser um número inteiro.")
        sys.exit(1)


if __name__ == "__main__":
    main()

