from __future__ import annotations
from typing import Iterable, List
from models.raw_headline import RawHeadline
from transformations.headline_normalizer import HeadlineNormalizer

def normalize_headlines(headlines: Iterable[RawHeadline]) -> List[RawHeadline]:
    normalizer = HeadlineNormalizer()
    return [normalizer.normalize(headline) for headline in headlines]