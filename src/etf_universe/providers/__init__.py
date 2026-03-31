from __future__ import annotations

from typing import Any

from etf_universe.contracts import EtfSpec, FetchResult
from etf_universe.providers.base import make_session
from etf_universe.providers.first_trust import fetch_first_trust
from etf_universe.providers.invesco import close_browser, fetch_invesco, launch_browser
from etf_universe.providers.ishares import fetch_ishares
from etf_universe.providers.ssga import fetch_ssga
from etf_universe.providers.vaneck import fetch_vaneck


PROVIDER_FETCHERS = {
    "ssga": fetch_ssga,
    "ishares": fetch_ishares,
    "vaneck": fetch_vaneck,
    "first_trust": fetch_first_trust,
    "invesco": fetch_invesco,
}


def fetch_with_provider(spec: EtfSpec, session: Any, page: Any = None) -> FetchResult:
    fetcher = PROVIDER_FETCHERS.get(spec.provider)
    if fetcher is None:
        raise ValueError(f"Unsupported provider: {spec.provider}")

    if spec.provider == "invesco":
        if page is None:
            raise ValueError("Invesco provider requires a browser page")
        return fetcher(spec, page)

    return fetcher(spec, session)
