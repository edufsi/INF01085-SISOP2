import hashlib
import json
from typing import Any


PROTOCOL_VERSION = 2
MAX_DATAGRAM = 1200
MAX_UINT64 = (1 << 64) - 1
SNAPSHOT_CHUNK_SIZE = 700


class ProtocolError(ValueError):
    pass


def message(message_type: str, **fields: Any) -> dict[str, Any]:
    return {"v": PROTOCOL_VERSION, "type": message_type, **fields}


def encode(payload: dict[str, Any]) -> bytes:
    if payload.get("v") != PROTOCOL_VERSION or not isinstance(payload.get("type"), str):
        raise ProtocolError("invalid protocol envelope")
    data = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(data) > MAX_DATAGRAM:
        raise ProtocolError(f"datagram exceeds {MAX_DATAGRAM} bytes")
    return data


def decode(data: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("invalid JSON datagram") from exc
    if not isinstance(payload, dict):
        raise ProtocolError("message must be an object")
    if payload.get("v") != PROTOCOL_VERSION or not isinstance(payload.get("type"), str):
        raise ProtocolError("unsupported protocol envelope")
    return payload


def require_uint64(value: Any, field: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProtocolError(f"{field} must be an integer")
    minimum = 1 if positive else 0
    if value < minimum or value > MAX_UINT64:
        raise ProtocolError(f"{field} is outside unsigned 64-bit range")
    return value


def snapshot_chunks(snapshot: dict[str, Any], transfer_id: str) -> list[dict[str, Any]]:
    raw = json.dumps(snapshot, separators=(",", ":"), sort_keys=True).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    parts = [
        raw[offset : offset + SNAPSHOT_CHUNK_SIZE].decode("utf-8")
        for offset in range(0, len(raw), SNAPSHOT_CHUNK_SIZE)
    ] or [""]
    return [
        message(
            "SNAPSHOT_CHUNK",
            transfer_id=transfer_id,
            index=index,
            count=len(parts),
            sha256=digest,
            data=part,
        )
        for index, part in enumerate(parts)
    ]


def assemble_snapshot(chunks: dict[int, str], count: int, digest: str) -> dict[str, Any]:
    if count <= 0 or set(chunks) != set(range(count)):
        raise ProtocolError("incomplete snapshot")
    raw = "".join(chunks[index] for index in range(count)).encode("utf-8")
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ProtocolError("snapshot checksum mismatch")
    try:
        snapshot = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ProtocolError("invalid snapshot JSON") from exc
    if not isinstance(snapshot, dict):
        raise ProtocolError("snapshot must be an object")
    return snapshot
