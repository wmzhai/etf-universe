import json
from dataclasses import asdict

import pyarrow as pa
import pyarrow.parquet as pq

from etf_universe.contracts import HoldingsMeta, NormalizedHoldingRow
from etf_universe.storage import write_meta, write_parquet


def test_write_parquet_persists_expected_rows(tmp_path) -> None:
    output_path = tmp_path / "SPY.parquet"
    rows = [
        NormalizedHoldingRow(symbol="AAPL", name="Apple", weight=6.1),
        NormalizedHoldingRow(symbol="BRK.B", name="Berkshire", weight=1.9),
    ]

    write_parquet(rows, output_path)

    table = pq.read_table(output_path)
    assert table.column("symbol").to_pylist() == ["AAPL", "BRK.B"]
    assert table.column("name").to_pylist() == ["Apple", "Berkshire"]
    assert table.column("weight").to_pylist() == [6.1, 1.9]
    expected_schema = pa.schema([
        ("symbol", pa.string()),
        ("name", pa.string()),
        ("weight", pa.float64()),
    ])
    assert table.schema == expected_schema

    parquet_file = pq.ParquetFile(output_path)
    row_group = parquet_file.metadata.row_group(0)
    column_compressions = {
        row_group.column(i).compression for i in range(row_group.num_columns)
    }
    assert column_compressions == {"ZSTD"}


def test_write_meta_persists_json_sidecar(tmp_path) -> None:
    output_path = tmp_path / "SPY.meta.json"
    meta = HoldingsMeta(
        etfSymbol="SPY",
        issuer="SSGA",
        fetchedAt="2026-03-31T12:00:00Z",
        sourceUrl="https://example.com/spy.xlsx",
        count=503,
        fundName="SPDR S&P 500 ETF Trust",
        expenseRatio=0.0945,
    )

    write_meta(meta, output_path)

    expected_payload = asdict(meta)
    expected_text = json.dumps(expected_payload, indent=2) + "\n"

    raw_text = output_path.read_text(encoding="utf-8")
    payload = json.loads(raw_text)
    assert payload == expected_payload
    assert "schemaVersion" not in payload
    assert "profile" not in payload
    assert "provider" not in payload
    assert "asOfDate" not in payload
    assert "sourceFormat" not in payload
    assert "fundType" not in payload
    assert "profileAsOfDate" not in payload
    assert "rowCount" not in payload
    assert "normalizedRowCount" not in payload
    assert "droppedRowCount" not in payload
    assert "distributionFrequency" not in payload
    assert "profileSourceUrl" not in payload
    assert payload["count"] == 503
    assert payload["fundName"] == "SPDR S&P 500 ETF Trust"
    assert payload["distributionYield"] is None
    assert raw_text == expected_text
    assert raw_text.startswith("{\n")
    assert "\n  \"" in raw_text
    assert raw_text.endswith("\n")
