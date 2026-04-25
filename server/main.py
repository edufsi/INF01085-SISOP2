import sys

from interface import iniciar_servidor


def main() -> None:
    if len(sys.argv) != 2:
        print("Uso incorreto!")
        print("Como usar: python main.py <PORTA>")
        print("Exemplo:   python main.py 50000")
        sys.exit(1)

    try:
        porta_escolhida = int(sys.argv[1])
        iniciar_servidor(porta_escolhida)
    except ValueError:
        print("Erro: A porta deve ser um número inteiro (ex: 50000).")
        sys.exit(1)


if __name__ == "__main__":
    main()

