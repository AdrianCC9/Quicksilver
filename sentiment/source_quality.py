from __future__ import annotations


TIER1_SOURCES = {
    "reuters",
    "bloomberg",
    "wsj",
    "wall street journal",
    "financial times",
    "ft",
    "cnbc",
    "associated press",
    "ap",
    "federal reserve",
    "sec",
    "u.s. treasury",
    "treasury",
    "white house",
}

TIER2_SOURCES = {
    "marketwatch",
    "seeking alpha",
    "benzinga",
    "yahoo finance",
    "nasdaq",
    "motley fool",
    "investopedia",
    "google news",
}


def classify_source(source: str) -> int:
    source_lower = source.strip().lower()
    if any(known_source in source_lower for known_source in TIER1_SOURCES):
        return 1
    if any(known_source in source_lower for known_source in TIER2_SOURCES):
        return 2
    return 3
