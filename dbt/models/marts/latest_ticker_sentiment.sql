{{ config(materialized='view') }}

with ranked_headlines as (
    select
        ticker,
        headline,
        source,
        url,
        published_at_utc,
        sentiment_label,
        positive_score,
        neutral_score,
        negative_score,
        compound_score,
        confidence,
        headline_age_hours,
        source_tier,

        row_number() over (
            partition by ticker
            order by published_at_utc desc
        ) as row_number

    from {{ source('quicksilver', 'scored_headlines') }}
)

select
    ticker,
    headline,
    source,
    url,
    published_at_utc,
    sentiment_label,
    positive_score,
    neutral_score,
    negative_score,
    compound_score,
    confidence,
    headline_age_hours,
    source_tier

from ranked_headlines

where row_number = 1
