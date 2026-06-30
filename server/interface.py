try:
    from .node import ServerNode
except ImportError:
    from node import ServerNode
import signal


def iniciar_servidor(
    porta: int,
    server_id: int,
    bind_host: str = "",
    discovery_address: tuple[str, int] | None = None,
    request_logging: bool = True,
) -> None:
    node = ServerNode(
        porta,
        server_id,
        bind_host,
        discovery_address=discovery_address,
        request_logging=request_logging,
    )
    terminating = False

    def handle_sigterm(_signum, _frame) -> None:
        nonlocal terminating
        if terminating:
            node.stop(graceful=False, force=True)
            return
        terminating = True
        if not node.stop(graceful=True):
            terminating = False

    signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        node.serve_forever()
    except KeyboardInterrupt:
        if not node.stop(graceful=True):
            print(
                "Encerramento adiado para preservar o cluster. "
                "Inicie outro servidor ou pressione Ctrl+C novamente para forçar.",
                flush=True,
            )
            try:
                while node.running.is_set():
                    import time

                    time.sleep(0.2)
            except KeyboardInterrupt:
                node.stop(graceful=False, force=True)
    finally:
        if node.running.is_set():
            node.stop(graceful=False, force=True)
