# AGENTS

- Keep this repository’s documentation, specifications, plans, and commit messages in English.
- Prefer `uv` for dependency installation, CLI execution, and tests (`uv sync`, `uv run etf-universe …`, `uv run pytest -v`).
- When publishing CLI examples, reference the flat public command hierarchy (`uv run etf-universe`, `uv run etf-universe list`, `uv run etf-universe fetch …`).
- Keep the README’s supported ETF list synchronized with `src/etf_universe/registry.py`; any change to the registry must be reflected inside the markers.
