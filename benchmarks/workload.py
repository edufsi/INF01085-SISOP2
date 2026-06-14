from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import random
from typing import Any


DEFAULT_CLIENTS = 4
DEFAULT_MIN_VALUE = 1
DEFAULT_MAX_VALUE = 100


def _write_client_values(
    path: Path,
    *,
    count: int,
    seed: int,
    minimum: int,
    maximum: int,
) -> dict[str, Any]:
    rng = random.Random(seed)
    digest = hashlib.sha256()
    total = 0
    with path.open("w", encoding="ascii", newline="\n") as output:
        for _ in range(count):
            value = rng.randint(minimum, maximum)
            encoded = f"{value}\n".encode("ascii")
            output.write(encoded.decode("ascii"))
            digest.update(encoded)
            total += value
    return {
        "file": path.name,
        "count": count,
        "sum": total,
        "sha256": digest.hexdigest(),
        "seed": seed,
    }


def generate_workloads(
    output_dir: Path,
    *,
    entries_per_client: int,
    seed: int,
    clients: int = DEFAULT_CLIENTS,
    minimum: int = DEFAULT_MIN_VALUE,
    maximum: int = DEFAULT_MAX_VALUE,
) -> dict[str, Any]:
    if entries_per_client <= 0:
        raise ValueError("entries_per_client must be positive")
    if clients <= 0:
        raise ValueError("clients must be positive")
    if minimum <= 0 or maximum < minimum:
        raise ValueError("invalid positive value range")
    output_dir.mkdir(parents=True, exist_ok=True)
    client_entries: list[dict[str, Any]] = []
    cumulative_sum = 0
    cumulative_count = 0
    for index in range(clients):
        client_seed = seed + index
        entry = _write_client_values(
            output_dir / f"client_{index + 1}.txt",
            count=entries_per_client,
            seed=client_seed,
            minimum=minimum,
            maximum=maximum,
        )
        entry["client_index"] = index
        cumulative_sum += int(entry["sum"])
        cumulative_count += int(entry["count"])
        entry["cumulative_grand_sum_after_client"] = cumulative_sum
        entry["cumulative_grand_count_after_client"] = cumulative_count
        client_entries.append(entry)
    manifest = {
        "format_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "seed": seed,
        "clients": clients,
        "entries_per_client": entries_per_client,
        "value_range": {"minimum": minimum, "maximum": maximum},
        "client_workloads": client_entries,
        "total_count": cumulative_count,
        "total_sum": cumulative_sum,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def load_and_verify_workloads(
    manifest_path: Path,
) -> tuple[dict[str, Any], list[list[int]]]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    workloads: list[list[int]] = []
    grand_count = 0
    grand_sum = 0
    for entry in manifest["client_workloads"]:
        path = manifest_path.parent / entry["file"]
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
            raise ValueError(f"checksum mismatch for {path}")
        values = [int(line) for line in raw.splitlines()]
        if len(values) != int(entry["count"]) or sum(values) != int(entry["sum"]):
            raise ValueError(f"count or sum mismatch for {path}")
        minimum = int(manifest["value_range"]["minimum"])
        maximum = int(manifest["value_range"]["maximum"])
        if any(value < minimum or value > maximum for value in values):
            raise ValueError(f"value outside configured range in {path}")
        workloads.append(values)
        grand_count += len(values)
        grand_sum += sum(values)
    if grand_count != int(manifest["total_count"]):
        raise ValueError("manifest total_count mismatch")
    if grand_sum != int(manifest["total_sum"]):
        raise ValueError("manifest total_sum mismatch")
    return manifest, workloads


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate deterministic integer lists and expected sums"
    )
    parser.add_argument("--output", type=Path, default=Path("benchmark-results/generated"))
    parser.add_argument("--entries", type=int, default=25_000)
    parser.add_argument("--seed", type=int, default=20260613)
    parser.add_argument("--clients", type=int, default=DEFAULT_CLIENTS)
    parser.add_argument("--minimum", type=int, default=DEFAULT_MIN_VALUE)
    parser.add_argument("--maximum", type=int, default=DEFAULT_MAX_VALUE)
    args = parser.parse_args()
    manifest = generate_workloads(
        args.output,
        entries_per_client=args.entries,
        seed=args.seed,
        clients=args.clients,
        minimum=args.minimum,
        maximum=args.maximum,
    )
    print(
        f"generated={manifest['total_count']} expected_sum={manifest['total_sum']} "
        f"manifest={args.output / 'manifest.json'}"
    )


if __name__ == "__main__":
    main()
