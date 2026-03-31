from __future__ import annotations

from etf_universe.contracts import EtfSpec


ETF_SPECS: dict[str, EtfSpec] = {
    "SPY": EtfSpec("SPY", "Layer 0", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"),
    "QQQ": EtfSpec("QQQ", "Layer 0", "Invesco", "invesco", "https://www.invesco.com/qqq-etf/en/about.html"),
    "DIA": EtfSpec("DIA", "Layer 0", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-dia.xlsx"),
    "IWM": EtfSpec("IWM", "Layer 0", "iShares", "ishares", "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv"),
    "XLK": EtfSpec("XLK", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlk.xlsx"),
    "XLF": EtfSpec("XLF", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlf.xlsx"),
    "XLE": EtfSpec("XLE", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xle.xlsx"),
    "XLV": EtfSpec("XLV", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlv.xlsx"),
    "XLY": EtfSpec("XLY", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xly.xlsx"),
    "XLP": EtfSpec("XLP", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlp.xlsx"),
    "XLI": EtfSpec("XLI", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xli.xlsx"),
    "XLB": EtfSpec("XLB", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlb.xlsx"),
    "XLU": EtfSpec("XLU", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlu.xlsx"),
    "XLRE": EtfSpec("XLRE", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlre.xlsx"),
    "XLC": EtfSpec("XLC", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlc.xlsx"),
    "SMH": EtfSpec("SMH", "Layer 2", "VanEck", "vaneck", "https://www.vaneck.com/us/en/investments/semiconductor-etf-smh/holdings/"),
    "IGV": EtfSpec("IGV", "Layer 2", "iShares", "ishares", "https://www.ishares.com/us/products/239771/ishares-north-american-techsoftware-etf/1467271812596.ajax?fileType=csv"),
    "KRE": EtfSpec("KRE", "Layer 2", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-kre.xlsx"),
    "KBE": EtfSpec("KBE", "Layer 2", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-kbe.xlsx"),
    "XOP": EtfSpec("XOP", "Layer 2", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xop.xlsx"),
    "OIH": EtfSpec("OIH", "Layer 2", "VanEck", "vaneck", "https://www.vaneck.com/us/en/investments/oil-services-etf-oih/holdings/"),
    "XBI": EtfSpec("XBI", "Layer 2", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xbi.xlsx"),
    "IHI": EtfSpec("IHI", "Layer 2", "iShares", "ishares", "https://www.ishares.com/us/products/239516/ishares-us-medical-devices-etf/1467271812596.ajax?fileType=csv"),
    "XRT": EtfSpec("XRT", "Layer 2", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xrt.xlsx"),
    "ITA": EtfSpec("ITA", "Layer 2", "iShares", "ishares", "https://www.ishares.com/us/products/239502/ishares-us-aerospace-defense-etf/1467271812596.ajax?fileType=csv"),
    "IYT": EtfSpec("IYT", "Layer 2", "iShares", "ishares", "https://www.ishares.com/us/products/239501/ishares-transportation-average-etf/1467271812596.ajax?fileType=csv"),
    "GDX": EtfSpec("GDX", "Layer 2", "VanEck", "vaneck", "https://www.vaneck.com/us/en/investments/gold-miners-etf-gdx/holdings/"),
    "FDN": EtfSpec("FDN", "Layer 2", "First Trust", "first_trust", "https://www.ftportfolios.com/Retail/Etf/EtfHoldings.aspx?Ticker=FDN"),
    "RSP": EtfSpec("RSP", "Breadth", "Invesco", "invesco", "https://www.invesco.com/us/en/financial-products/etfs/invesco-sp-500-equal-weight-etf.html"),
    "QQEW": EtfSpec("QQEW", "Breadth", "First Trust", "first_trust", "https://www.ftportfolios.com/Retail/Etf/EtfHoldings.aspx?Ticker=QQEW"),
}


def list_supported_symbols() -> list[str]:
    return sorted(ETF_SPECS)


def parse_symbols_arg(raw: str) -> list[str]:
    seen: set[str] = set()
    symbols: list[str] = []

    for item in raw.split(","):
        symbol = item.strip().upper()
        if not symbol:
            continue
        if symbol not in ETF_SPECS:
            raise SystemExit(f"Unknown ETF symbols: {symbol}")
        if symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)

    if not symbols:
        raise SystemExit("No ETF symbols supplied")

    return symbols


def get_specs(symbols: list[str]) -> list[EtfSpec]:
    return [ETF_SPECS[symbol] for symbol in symbols]
