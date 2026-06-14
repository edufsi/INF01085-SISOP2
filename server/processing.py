"""Compatibility module.

Request processing now lives in :mod:`server.node` because client commits,
replication, membership changes, and elections share one coordinated state
machine.
"""

try:
    from .node import ServerNode
except ImportError:
    from node import ServerNode

__all__ = ["ServerNode"]
