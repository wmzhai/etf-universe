from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from etf_universe.contracts import HoldingsMeta, NormalizedHoldingRow


PARQUET_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("name", pa.string()),
        ("weight", pa.float64()),
    ]
)


def write_parquet(rows: list[NormalizedHoldingRow], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist([asdict(row) for row in rows], schema=PARQUET_SCHEMA)
    pq.write_table(table, output_path, compression="zstd")


def write_meta(meta: HoldingsMeta, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(f"{json.dumps(asdict(meta), indent=2)}\n", encoding="utf-8")
