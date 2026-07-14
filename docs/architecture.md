# Architecture

## Scope and sources of truth

`etf-universe` is an agent-agnostic Python package that exports the latest
holdings snapshot for a curated ETF universe. Humans and automation use the
same flat CLI; there is no separate agent integration layer.

The project has three durable sources of truth:

- The README defines installation, public CLI usage, and the external output
  contract. Its marked supported-ETF block is a synchronized public mirror,
  not the canonical registry.
- The `etf_universe.registry` module is the source of truth for the supported
  ETF universe, provider assignments, issuers, and source URLs.
- `pyproject.toml` defines runtime and development dependencies.

The package does not discover arbitrary ETFs or retain historical snapshots.
Callers that need history must archive each run's output externally.

## Execution pipeline

A fetch follows one registry-driven pipeline:

1. The CLI resolves the requested symbols against the registry. An unknown
   ETF is rejected before any network request begins.
2. Each provider adapter fetches and parses its upstream holdings into a
   common result containing source rows, the provider's snapshot date, source
   information, and best-effort fund profile data.
3. The normalization layer applies local row filters and collects candidate
   constituent symbols across the entire run.
4. The validator deduplicates that shared candidate universe and, when Alpaca
   credentials are configured, validates it in batches.
5. Each provider result is normalized against the same valid-symbol set and
   written in the ETF order requested by the caller.

The provider snapshot date is used in runtime status and logging. It is not a
field in the current metadata sidecar; the README documents the persisted
metadata fields.

## Provider adapters

Every provider adapter returns the same fetch-result contract, keeping
provider-specific formats and enrichment logic outside CLI orchestration.

| Provider | Holdings strategy | Profile strategy |
| --- | --- | --- |
| ARK | Official CSV | Best-effort enrichment from ARK fund data |
| SSGA | Official XLSX | Fields available in the workbook |
| iShares | Official CSV | CSV fields plus the product page |
| VanEck | Product page followed by its dataset endpoint | Fields in the dataset |
| First Trust | Holdings HTML | Fund summary page |
| Invesco | Playwright-assisted JSON discovery and retrieval | Product page content |

Invesco is the only browser-backed provider. It requires Chromium to be
installed for Playwright.

## Concurrency and resource ownership

Non-Invesco ETFs fetch concurrently with a maximum of 16 workers. Concurrent
tasks own dedicated HTTP sessions so request state is not shared across
threads. Invesco ETFs run serially on one Playwright page within one browser
session. The browser-backed track can overlap the non-browser worker pool, and
all results are reassembled in request order before validation and storage.

The current operational defaults are:

| Setting | Default |
| --- | ---: |
| Non-browser fetch workers | 16 |
| `requests` and Alpaca HTTP timeout | 60 seconds |
| Alpaca symbols per batch | 200 |
| Concurrent Alpaca batches | 8 |

These are implementation defaults, not public CLI options. If they change,
update this document together with the relevant tests.

Playwright uses separate browser timeouts; Invesco page navigation currently
allows up to 120 seconds.

## Normalization and output invariants

Before a constituent can be stored, the normalization layer:

- trims and uppercases its symbol;
- converts slash-form share classes to dot form, such as `BRK/B` to `BRK.B`;
- rejects symbols outside the supported equity-symbol shape;
- drops cash, currency placeholders, non-holding rows, and rows identified as
  cash-like by asset class or security type; and
- applies the run-wide Alpaca valid-symbol set when remote validation is
  enabled.

A fetch fails if any requested ETF has no usable holdings after normalization.
This prevents a successful-looking empty snapshot from replacing valid data.

Writes go directly to their final paths. They are neither atomic per file nor
transactional across the requested ETF set, so an interruption can leave a
partially written file. If a later ETF fails during normalization or storage,
files for earlier ETFs may already contain the new snapshot while untouched
files may still contain a previous run. Consumers that require an all-or-
nothing snapshot should write to a new directory and publish that directory
only after the command succeeds.

Parquet files use the fixed `symbol`, `name`, and `weight` schema with Zstandard
compression. Metadata is a top-level JSON object containing snapshot identity
and source fields plus best-effort profile fields. Unavailable profile values
are written as `null`; provider-only profile fields that are not part of the
public contract are not persisted.

## Alpaca validation

Alpaca validation is optional and runs after all provider fetches:

- Process environment variables take precedence over values loaded from
  `.env`.
- When either Alpaca credential is missing, remote validation is disabled and
  every locally eligible normalized symbol is retained.
- Candidate symbols are deduplicated once for the complete run, divided into
  batches of up to 200, and validated with up to 8 batches in flight.
- A successful response validates only symbols present in its `quotes` map;
  omitted symbols are invalid.
- On an Alpaca `400` or `404` invalid-symbol response, the validator isolates
  the reported symbol, removes it, and retries the remainder of the batch.
- Other HTTP failures and ambiguous invalid-symbol responses fail the fetch.

Dot-form share classes remain in dot form in both Alpaca requests and stored
output.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `etf_universe.cli` | Argument parsing, orchestration, concurrency, configuration loading, and resource cleanup |
| `etf_universe.registry` | Curated ETF definitions, symbol parsing, and request-order lookup |
| `etf_universe.contracts` | Shared immutable fetch, row, profile, and metadata contracts |
| `etf_universe.providers` | Provider dispatch plus provider-specific fetch and parse logic |
| `etf_universe.normalization` | Text cleanup, symbol normalization, local filtering, and storage shaping |
| `etf_universe.validation` | Alpaca batching, concurrency, retry isolation, and credential fallback |
| `etf_universe.profile` | Shared parsing and merging helpers for best-effort profile enrichment |
| `etf_universe.storage` | Fixed-schema Parquet and JSON sidecar writes |
| `etf_universe.runtime_logging` | Structured `stderr` events and elapsed-time reporting |

Keeping these boundaries stable lets provider parsers evolve without changing
the public CLI or storage schema.

## Maintenance invariants

When changing the supported universe, update the registry and the README's
generated supported-ETF block in the same change. Confirm that the selected
adapter supports the new symbol; some adapters, including Invesco, maintain
symbol-specific endpoint mappings. A new provider also needs a dispatch entry,
parser tests based on representative upstream data, and CLI coverage for its
resource requirements.

Use `uv run pytest -v` for the automated test suite. For behavior or output
format changes, the primary confidence gate is a real full-universe run with
`uv run etf-universe`: inspect the runtime logs, every generated metadata
sidecar, and the Parquet schemas and rows, then have Codex independently review
the observed logs and output shape. Keep published examples on the flat CLI
and document only the current Alpaca validation flow.
