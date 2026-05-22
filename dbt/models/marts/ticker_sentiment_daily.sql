{{ config(materialized='view') }}

select
    ticker,
    cast(published_at_utc as date) as sentiment_date,

    count(*) as headline_count,

    avg(compound_score) as avg_compound_score,
    avg(confidence) as avg_confidence,

    sum(case when sentiment_label = 'positive' then 1 else 0 end) as positive_headline_count,
    sum(case when sentiment_label = 'neutral' then 1 else 0 end) as neutral_headline_count,
    sum(case when sentiment_label = 'negative' then 1 else 0 end) as negative_headline_count,

    avg(positive_score) as avg_positive_score,
    avg(neutral_score) as avg_neutral_score,
    avg(negative_score) as avg_negative_score,

    min(published_at_utc) as first_headline_at_utc,
    max(published_at_utc) as latest_headline_at_utc

from {{ source('quicksilver', 'scored_headlines') }}

group by
    ticker,
    cast(published_at_utc as date)
