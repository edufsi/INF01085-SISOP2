#!/usr/bin/env python3
"""Generate random integer partitions for parallel sum testing."""

from __future__ import annotations

import argparse
import random
from pathlib import Path


LIST_SIZE = 100_000
PARTITION_COUNT = 4
MIN_VALUE = 1
MAX_VALUE = 50


def write_numbers(path: Path, numbers: list[int]) -> None:
    path.write_text("\n".join(str(number) for number in numbers) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate one 100k-element list and split it into 4 equal files."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Directory where the full list and partition files will be written.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for reproducible test data.",
    )
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    numbers = [random.randint(MIN_VALUE, MAX_VALUE) for _ in range(LIST_SIZE)]
    total = sum(numbers)
    partition_size = LIST_SIZE // PARTITION_COUNT

    full_list_path = args.output_dir / "full_list.txt"
    write_numbers(full_list_path, numbers)

    partition_sums = []
    for index in range(PARTITION_COUNT):
        start = index * partition_size
        end = start + partition_size
        partition = numbers[start:end]
        partition_sums.append(sum(partition))

        partition_path = args.output_dir / f"partition_{index + 1}.txt"
        write_numbers(partition_path, partition)

    print(f"Generated {LIST_SIZE} numbers in {full_list_path}")
    print(f"Expected total sum: {total}")
    for index, partition_sum in enumerate(partition_sums, start=1):
        print(f"partition_{index}.txt sum: {partition_sum}")
    print(f"Partition sums total: {sum(partition_sums)}")


if __name__ == "__main__":
    main()
