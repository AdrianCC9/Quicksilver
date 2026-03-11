from dataclasses import dataclass

@dataclass(slots=True)
class SentimentResult:
    """
    Represents the output of FinBERT sentiment scoring.
    """

    label: str
    positive_score: float
    neutral_score: float
    negative_score: float