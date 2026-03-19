# tests/test_finbert_scorer.py

from __future__ import annotations
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
# ↑ unittest.mock lets us FAKE the FinBERT model entirely.
#   This is critical — we do not want tests to download a 400MB model
#   every time we run them. Instead we tell Python "pretend the pipeline
#   returned this fake data" and test our logic against that.

from models.raw_headline import RawHeadline
from models.sentiment_result import SentimentResult
from models.scored_headline import ScoredHeadline


# ---------------------------------------------------------------------------
# SHARED TEST DATA
# ---------------------------------------------------------------------------
# We define fake FinBERT output once here and reuse it across all tests.
# This is exactly what the real Hugging Face pipeline returns with
# return_all_scores=True — a list containing a list of dicts.

FAKE_FINBERT_OUTPUT_POSITIVE = [[
    {"label": "positive", "score": 0.91},
    {"label": "neutral",  "score": 0.07},
    {"label": "negative", "score": 0.02},
]]

FAKE_FINBERT_OUTPUT_NEGATIVE = [[
    {"label": "positive", "score": 0.02},
    {"label": "neutral",  "score": 0.11},
    {"label": "negative", "score": 0.87},
]]

FAKE_FINBERT_OUTPUT_NEUTRAL = [[
    {"label": "positive", "score": 0.15},
    {"label": "neutral",  "score": 0.75},
    {"label": "negative", "score": 0.10},
]]


def make_raw_headline(
    ticker: str = "AAPL",
    headline: str = "Apple beats earnings expectations",
    source: str = "Reuters",
    url: str = "https://reuters.com/apple",
    hours_ago: float = 2.0,
    summary: str | None = None,
) -> RawHeadline:
    """
    Helper function that builds a RawHeadline for use in tests.
    hours_ago lets us control how old the headline appears to be.
    """
    published = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return RawHeadline(
        ticker=ticker,
        headline=headline,
        source=source,
        url=url,
        published_at_utc=published,
        summary=summary,
    )


# ---------------------------------------------------------------------------
# FIXTURE — builds a FinBERTScorer with the real pipeline MOCKED OUT
# ---------------------------------------------------------------------------
# A pytest "fixture" is a reusable setup block.
# @pytest.fixture means: "before each test that asks for `scorer`,
# run this function and pass the result in as the argument."

@pytest.fixture
def scorer():
    """
    Returns a FinBERTScorer where the Hugging Face pipeline is replaced
    with a MagicMock. This means no model is downloaded and no GPU is used.
    Each individual test will set what the mock returns.
    """
    # patch() temporarily replaces transformers.pipeline with a fake version
    # for the duration of the test only — it gets restored after.
    with patch("sentiment.finbert_scorer.pipeline") as mock_pipeline:
        mock_pipeline.return_value = MagicMock()

        # Now import and create the scorer — it will use our fake pipeline
        from sentiment.finbert_scorer import FinBERTScorer
        s = FinBERTScorer()
        yield s
        # ↑ yield instead of return keeps the patch active while the test runs


# ---------------------------------------------------------------------------
# TESTS: _normalize_label
# ---------------------------------------------------------------------------

class TestNormalizeLabel:
    """Tests for the static label normalization helper."""

    def test_lowercase(self, scorer):
        assert scorer._normalize_label("Positive") == "positive"

    def test_already_lowercase(self, scorer):
        assert scorer._normalize_label("negative") == "negative"

    def test_uppercase(self, scorer):
        assert scorer._normalize_label("NEUTRAL") == "neutral"

    def test_strips_whitespace(self, scorer):
        assert scorer._normalize_label("  positive  ") == "positive"


# ---------------------------------------------------------------------------
# TESTS: _classify_source
# ---------------------------------------------------------------------------

class TestClassifySource:
    """
    Tests for source tier classification.
    Tier 1 = major financial press
    Tier 2 = financial media / blogs
    Tier 3 = unknown / everything else
    """

    def test_tier1_reuters(self, scorer):
        assert scorer._classify_source("Reuters") == 1

    def test_tier1_bloomberg(self, scorer):
        assert scorer._classify_source("Bloomberg") == 1

    def test_tier1_wsj(self, scorer):
        assert scorer._classify_source("WSJ") == 1

    def test_tier1_case_insensitive(self, scorer):
        # Source names from Finnhub may come in any casing
        assert scorer._classify_source("CNBC") == 1

    def test_tier2_marketwatch(self, scorer):
        assert scorer._classify_source("MarketWatch") == 2

    def test_tier2_benzinga(self, scorer):
        assert scorer._classify_source("Benzinga") == 2

    def test_tier2_seeking_alpha(self, scorer):
        assert scorer._classify_source("Seeking Alpha") == 2

    def test_tier3_unknown(self, scorer):
        assert scorer._classify_source("Some Random Blog") == 3

    def test_tier3_empty_string(self, scorer):
        assert scorer._classify_source("") == 3


# ---------------------------------------------------------------------------
# TESTS: _calculate_age_hours
# ---------------------------------------------------------------------------

class TestCalculateAgeHours:
    """Tests for headline age calculation."""

    def test_two_hours_ago(self, scorer):
        published = datetime.now(timezone.utc) - timedelta(hours=2)
        age = scorer._calculate_age_hours(published)
        # Should be approximately 2.0 — we allow a small margin for
        # the tiny amount of time the test itself takes to run
        assert 1.99 <= age <= 2.01

    def test_24_hours_ago(self, scorer):
        published = datetime.now(timezone.utc) - timedelta(hours=24)
        age = scorer._calculate_age_hours(published)
        assert 23.99 <= age <= 24.01

    def test_result_is_rounded_to_2_decimal_places(self, scorer):
        published = datetime.now(timezone.utc) - timedelta(hours=1, minutes=30)
        age = scorer._calculate_age_hours(published)
        # 1 hour 30 minutes = 1.5 hours
        assert 1.49 <= age <= 1.51


# ---------------------------------------------------------------------------
# TESTS: score_text
# ---------------------------------------------------------------------------

class TestScoreText:
    """Tests for the core FinBERT scoring logic."""

    def test_positive_headline(self, scorer):
        scorer.classifier.return_value = FAKE_FINBERT_OUTPUT_POSITIVE
        result = scorer.score_text("Apple crushes earnings expectations")

        assert isinstance(result, SentimentResult)
        assert result.label == "positive"
        assert result.positive_score == pytest.approx(0.91)
        assert result.neutral_score == pytest.approx(0.07)
        assert result.negative_score == pytest.approx(0.02)
        # ↑ pytest.approx() is used for floats because floating point
        #   arithmetic means 0.91 might actually be 0.9099999999 internally.
        #   approx() allows a tiny margin of error.

    def test_negative_headline(self, scorer):
        scorer.classifier.return_value = FAKE_FINBERT_OUTPUT_NEGATIVE
        result = scorer.score_text("CEO arrested for massive fraud scheme")

        assert result.label == "negative"
        assert result.negative_score == pytest.approx(0.87)

    def test_neutral_headline(self, scorer):
        scorer.classifier.return_value = FAKE_FINBERT_OUTPUT_NEUTRAL
        result = scorer.score_text("Company files quarterly report")

        assert result.label == "neutral"

    def test_compound_score_positive(self, scorer):
        scorer.classifier.return_value = FAKE_FINBERT_OUTPUT_POSITIVE
        result = scorer.score_text("Apple crushes earnings")
        # compound = positive - negative = 0.91 - 0.02 = 0.89
        assert result.compound_score == pytest.approx(0.89, abs=1e-4)

    def test_compound_score_negative(self, scorer):
        scorer.classifier.return_value = FAKE_FINBERT_OUTPUT_NEGATIVE
        result = scorer.score_text("CEO arrested for fraud")
        # compound = 0.02 - 0.87 = -0.85
        assert result.compound_score == pytest.approx(-0.85, abs=1e-4)

    def test_confidence_is_max_score(self, scorer):
        scorer.classifier.return_value = FAKE_FINBERT_OUTPUT_POSITIVE
        result = scorer.score_text("Apple crushes earnings")
        # confidence should be 0.91 — the highest of the three scores
        assert result.confidence == pytest.approx(0.91)

    def test_empty_string_raises(self, scorer):
        with pytest.raises(ValueError, match="cannot be empty"):
            scorer.score_text("")
        # ↑ pytest.raises() asserts that this code DOES raise a ValueError.
        #   match= checks the error message contains that phrase.

    def test_whitespace_only_raises(self, scorer):
        with pytest.raises(ValueError, match="cannot be empty"):
            scorer.score_text("     ")

    def test_flat_list_output_handled(self, scorer):
        # Tests the case where Hugging Face returns a flat list instead
        # of a list of lists (Structure B from our earlier explanation)
        flat_output = [
            {"label": "positive", "score": 0.91},
            {"label": "neutral",  "score": 0.07},
            {"label": "negative", "score": 0.02},
        ]
        scorer.classifier.return_value = flat_output
        result = scorer.score_text("Apple beats expectations")
        assert result.label == "positive"


# ---------------------------------------------------------------------------
# TESTS: score_headline
# ---------------------------------------------------------------------------

class TestScoreHeadline:
    """Tests that score_headline correctly extracts the text from RawHeadline."""

    def test_passes_headline_text_to_score_text(self, scorer):
        scorer.classifier.return_value = FAKE_FINBERT_OUTPUT_POSITIVE
        headline = make_raw_headline(headline="Apple beats earnings expectations")
        result = scorer.score_headline(headline)

        assert isinstance(result, SentimentResult)
        assert result.label == "positive"


# ---------------------------------------------------------------------------
# TESTS: score_batch
# ---------------------------------------------------------------------------

class TestScoreBatch:
    """Tests the main pipeline method that processes a list of headlines."""

    def test_returns_list_of_scored_headlines(self, scorer):
        scorer.classifier.return_value = FAKE_FINBERT_OUTPUT_POSITIVE
        headlines = [make_raw_headline(), make_raw_headline(ticker="MSFT")]
        results = scorer.score_batch(headlines)

        assert isinstance(results, list)
        assert all(isinstance(r, ScoredHeadline) for r in results)

    def test_correct_number_of_results(self, scorer):
        scorer.classifier.return_value = FAKE_FINBERT_OUTPUT_POSITIVE
        headlines = [make_raw_headline() for _ in range(5)]
        results = scorer.score_batch(headlines)
        assert len(results) == 5

    def test_ticker_preserved(self, scorer):
        scorer.classifier.return_value = FAKE_FINBERT_OUTPUT_POSITIVE
        headline = make_raw_headline(ticker="NVDA")
        results = scorer.score_batch([headline])
        assert results[0].ticker == "NVDA"

    def test_source_tier_assigned(self, scorer):
        scorer.classifier.return_value = FAKE_FINBERT_OUTPUT_POSITIVE
        headline = make_raw_headline(source="Reuters")
        results = scorer.score_batch([headline])
        assert results[0].source_tier == 1

    def test_age_hours_populated(self, scorer):
        scorer.classifier.return_value = FAKE_FINBERT_OUTPUT_POSITIVE
        headline = make_raw_headline(hours_ago=3.0)
        results = scorer.score_batch([headline])
        assert 2.99 <= results[0].headline_age_hours <= 3.01

    def test_failed_headline_is_skipped(self, scorer):
        # Simulate one headline causing an exception inside scoring.
        # score_batch should skip it and continue — not crash.
        scorer.classifier.side_effect = [
            Exception("model exploded"),   # first headline fails
            FAKE_FINBERT_OUTPUT_POSITIVE,  # second headline succeeds
        ]
        headlines = [make_raw_headline(), make_raw_headline(ticker="MSFT")]
        results = scorer.score_batch(headlines)

        # Only 1 result because the first was skipped
        assert len(results) == 1
        assert results[0].ticker == "MSFT"

    def test_empty_list_returns_empty(self, scorer):
        results = scorer.score_batch([])
        assert results == []

    def test_compound_score_stored_correctly(self, scorer):
        scorer.classifier.return_value = FAKE_FINBERT_OUTPUT_NEGATIVE
        headline = make_raw_headline()
        results = scorer.score_batch([headline])
        # compound = 0.02 - 0.87 = -0.85
        assert results[0].compound_score == pytest.approx(-0.85, abs=1e-4)