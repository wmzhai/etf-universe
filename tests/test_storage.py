import json

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
    assert table.column("weight").to_pylist() == [6.1, 1.9]


def test_write_meta_persists_json_sidecar(tmp_path) -> None:
    output_path = tmp_path / "SPY.meta.json"
    meta = HoldingsMeta(
        schemaVersion="2026-03-31.etf-universe-meta.v1",
        etfSymbol="SPY",
        issuer="SSGA",
        provider="ssga",
        asOfDate="2026-03-28",
        fetchedAt="2026-03-31T12:00:00Z",
        sourceUrl="https://example.com/spy.xlsx",
        sourceFormat="xlsx",
        rowCount=503,
        normalizedRowCount=503,
        droppedRowCount=0,
    )

    write_meta(meta, output_path)

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["etfSymbol"] == "SPY"
    assert payload["sourceFormat"] == "xlsx"
