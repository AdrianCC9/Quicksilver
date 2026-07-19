from __future__ import annotations
from datetime import datetime, timezone
import hashlib
import re
from models.raw_headline import RawHeadline

class HeadlineNormalizer:
    @staticmethod
    def _clean_text(value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", value).strip()
    
    @staticmethod
    def _clean_optional_text(value: str | None) -> str | None:
        cleaned = HeadlineNormalizer._clean_text(value)
        return cleaned if cleaned else None
    
    @staticmethod
    def _normalize_timestamp(value: datetime | int | float | str) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
        
        if isinstance(value, str):
            value = value.strip()

            if value.isdigit():
                return datetime.fromtimestamp(int(value), tz=timezone.utc)
            
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        
        raise TypeError(f"Unsupported timestamp type: {type(value)}")
    
    def normalize(self, headline: RawHeadline) -> RawHeadline:
        return RawHeadline(
            ticker=self._clean_text(headline.ticker).upper(),
            headline=self._clean_text(headline.headline),
            source=self._clean_text(headline.source) or "unknown",
            published_at_utc=self._normalize_timestamp(headline.published_at_utc),
            summary=self._clean_optional_text(headline.summary),
            url=self._clean_text(headline.url),
            category=self._clean_text(headline.category).lower() or "financial",
            topic=self._clean_optional_text(headline.topic),
            industry=self._clean_optional_text(headline.industry),
        )
    
    @staticmethod
    def build_content_hash(headline: RawHeadline) -> str:
        hash_input = "||".join(
            [
                headline.ticker,
                headline.headline,
                headline.source,
                headline.url,
                headline.published_at_utc.isoformat(),
                headline.summary or "",
                headline.category,
                headline.topic or "",
                headline.industry or "",
            ]
        )
        return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()
