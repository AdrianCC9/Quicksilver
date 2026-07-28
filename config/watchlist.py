"""
Static S&P 500 ticker universe for the Quicksilver pipeline.

The symbol list is checked into the repo so local runs, tests, the dashboard,
and optional Kafka/Snowflake/dbt/Airflow demos all use the same runtime universe
without scraping. Source snapshot:
https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv
"""

SP500_TICKERS: tuple[str, ...] = (
    "MMM", "AOS", "ABT", "ABBV", "ACN", "ADBE", "AMD", "AES",
    "AFL", "A", "APD", "ABNB", "AKAM", "ALB", "ARE", "ALGN",
    "ALLE", "LNT", "ALL", "GOOGL", "GOOG", "MO", "AMZN", "AMCR",
    "AEE", "AEP", "AXP", "AIG", "AMT", "AWK", "AMP", "AME",
    "AMGN", "APH", "ADI", "AON", "APA", "APO", "AAPL", "AMAT",
    "APP", "APTV", "ACGL", "ADM", "ARES", "ANET", "AJG", "AIZ",
    "T", "ATO", "ADSK", "ADP", "AZO", "AVB", "AVY", "AXON",
    "BKR", "BALL", "BAC", "BAX", "BDX", "BRK.B", "BBY", "TECH",
    "BIIB", "BLK", "BX", "XYZ", "BNY", "BA", "BKNG", "BSX",
    "BMY", "AVGO", "BR", "BRO", "BF.B", "BLDR", "BG", "BXP",
    "CHRW", "CDNS", "CPT", "COF", "CAH", "CCL", "CARR", "CVNA",
    "CASY", "CAT", "CBOE", "CBRE", "CDW", "COR", "CNC", "CNP",
    "CF", "CRL", "SCHW", "CHTR", "CVX", "CMG", "CB", "CHD",
    "CIEN", "CI", "CINF", "CTAS", "CSCO", "C", "CFG", "CLX",
    "CME", "CMS", "KO", "CTSH", "COHR", "COIN", "CL", "CMCSA",
    "FIX", "COP", "ED", "STZ", "CEG", "COO", "CPRT", "GLW",
    "CPAY", "CTVA", "CSGP", "COST", "CRH", "CRWD", "CCI", "CSX",
    "CMI", "CVS", "DHR", "DRI", "DDOG", "DVA", "DECK", "DE",
    "DELL", "DAL", "DVN", "DXCM", "FANG", "DLR", "DG", "DLTR",
    "D", "DPZ", "DASH", "DOV", "DOW", "DHI", "DTE", "DUK",
    "DD", "ETN", "EBAY", "ECHO", "ECL", "EIX", "EW", "EA",
    "ELV", "EME", "EMR", "ETR", "EOG", "EQT", "EFX", "EQIX",
    "EQR", "ERIE", "ESS", "EL", "EG", "EVRG", "ES", "EXC",
    "EXE", "EXPE", "EXPD", "EXR", "XOM", "FFIV", "FDS", "FICO",
    "FAST", "FRT", "FDX", "FDXF", "FIS", "FITB", "FSLR", "FE",
    "FISV", "FLEX", "F", "FTNT", "FTV", "FOXA", "FOX", "BEN",
    "FCX", "GRMN", "IT", "GE", "GEHC", "GEV", "GEN", "GNRC",
    "GD", "GIS", "GM", "GPC", "GILD", "GPN", "GL", "GDDY",
    "GS", "HAL", "HIG", "HAS", "HCA", "DOC", "HSIC", "HSY",
    "HPE", "HLT", "HD", "HONA", "HON", "HRL", "HST", "HWM",
    "HPQ", "HUBB", "HUM", "HBAN", "HII", "IBM", "IEX", "IDXX",
    "ITW", "INCY", "IR", "PODD", "INTC", "IBKR", "ICE", "IFF",
    "IP", "INTU", "ISRG", "IVZ", "INVH", "IQV", "IRM", "JBHT",
    "JBL", "JKHY", "J", "JNJ", "JCI", "JPM", "KVUE", "KDP",
    "KEY", "KEYS", "KMB", "KIM", "KMI", "KKR", "KLAC", "KHC",
    "KR", "LHX", "LH", "LRCX", "LVS", "LDOS", "LEN", "LII",
    "LLY", "LIN", "LYV", "LMT", "L", "LOW", "LULU", "LITE",
    "LYB", "MTB", "MPC", "MAR", "MRSH", "MLM", "MRVL", "MAS",
    "MA", "MKC", "MCD", "MCK", "MDT", "MRK", "META", "MET",
    "MTD", "MGM", "MCHP", "MU", "MSFT", "MAA", "MRNA", "TAP",
    "MDLZ", "MPWR", "MNST", "MCO", "MS", "MOS", "MSI", "MSCI",
    "NDAQ", "NTAP", "NFLX", "NEM", "NWSA", "NWS", "NEE", "NKE",
    "NI", "NDSN", "NSC", "NTRS", "NOC", "NCLH", "NRG", "NUE",
    "NVDA", "NVR", "NXPI", "ORLY", "OXY", "ODFL", "OMC", "ON",
    "OKE", "ORCL", "OTIS", "PCAR", "PKG", "PLTR", "PANW", "PSKY",
    "PH", "PAYX", "PYPL", "PNR", "PEP", "PFE", "PCG", "PM",
    "PSX", "PNW", "PNC", "PPG", "PPL", "PFG", "PG", "PGR",
    "PLD", "PRU", "PEG", "PTC", "PSA", "PHM", "PWR", "QCOM",
    "DGX", "Q", "RL", "RJF", "RTX", "O", "REG", "REGN",
    "RF", "RSG", "RMD", "RVTY", "HOOD", "ROK", "ROL", "ROP",
    "ROST", "RCL", "SPGI", "CRM", "SNDK", "SBAC", "SLB", "STX",
    "SRE", "NOW", "SHW", "SPG", "SWKS", "SJM", "SW", "SNA",
    "SOLV", "SO", "LUV", "SWK", "SBUX", "STT", "STLD", "STE",
    "SYK", "SMCI", "SYF", "SNPS", "SYY", "TMUS", "TROW", "TTWO",
    "TPR", "TRGP", "TGT", "TEL", "TDY", "TER", "TSLA", "TXN",
    "TPL", "TXT", "TMO", "TJX", "TKO", "TTD", "TSCO", "TT",
    "TDG", "TRV", "TRMB", "TFC", "TYL", "TSN", "USB", "UBER",
    "UDR", "ULTA", "UNP", "UAL", "UPS", "URI", "UNH", "UHS",
    "VLO", "VEEV", "VTR", "VLTO", "VRSN", "VRSK", "VZ", "VRTX",
    "VRT", "VTRS", "VICI", "V", "VST", "VMC", "WRB", "GWW",
    "WAB", "WMT", "DIS", "WBD", "WM", "WAT", "WEC", "WFC",
    "WELL", "WST", "WDC", "WY", "WSM", "WMB", "WTW", "WDAY",
    "WYNN", "XEL", "XYL", "YUM", "ZBRA", "ZBH", "ZTS",
)

SP500_TICKER_SET = frozenset(SP500_TICKERS)

# Backward-compatible names for older tests/scripts. They intentionally resolve
# to the same S&P 500 universe rather than maintaining separate 50/100 lists.
TOP_50_EQUITY_TICKERS = SP500_TICKERS
EXPANDED_EQUITY_TICKERS = SP500_TICKERS


def normalize_ticker(ticker: str) -> str:
    return ticker.strip().upper()


def is_sp500_ticker(ticker: str) -> bool:
    return normalize_ticker(ticker) in SP500_TICKER_SET


def filter_to_sp500_tickers(tickers: list[str] | tuple[str, ...]) -> list[str]:
    return [
        ticker
        for ticker in dict.fromkeys(normalize_ticker(ticker) for ticker in tickers)
        if ticker in SP500_TICKER_SET
    ]


def get_default_watchlist() -> list[str]:
    return list(SP500_TICKERS)


def get_sp500_watchlist() -> list[str]:
    return get_default_watchlist()


def get_expanded_watchlist() -> list[str]:
    return get_default_watchlist()
