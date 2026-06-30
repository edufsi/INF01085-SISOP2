import errno


TRANSIENT_NETWORK_ERRNOS = {
    value
    for value in (
        getattr(errno, "ENETUNREACH", None),
        getattr(errno, "ENETDOWN", None),
        getattr(errno, "EHOSTUNREACH", None),
        getattr(errno, "EHOSTDOWN", None),
    )
    if value is not None
}


def is_transient_network_error(exc: OSError) -> bool:
    return exc.errno in TRANSIENT_NETWORK_ERRNOS
