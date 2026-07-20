{{ config(materialized='view') }}

with scored as (
    select
        ticker,
        cast(published_at_utc as date) as sentiment_date,
        sentiment_label,
        positive_score,
        neutral_score,
        negative_score,
        compound_score,
        confidence,
        source_tier,

        case
            when source_tier = 1 then 3
            when source_tier = 2 then 2
            else 1
        end as source_weight,

        published_at_utc
    from {{ source('quicksilver', 'scored_headlines') }}
)

select
    ticker,
    sentiment_date,

    count(*) as headline_count,

    avg(compound_score) as avg_compound_score,
    avg(confidence) as avg_confidence,

    sum(case when sentiment_label = 'positive' then 1 else 0 end) as positive_headline_count,
    sum(case when sentiment_label = 'neutral' then 1 else 0 end) as neutral_headline_count,
    sum(case when sentiment_label = 'negative' then 1 else 0 end) as negative_headline_count,

    avg(positive_score) as avg_positive_score,
    avg(neutral_score) as avg_neutral_score,
    avg(negative_score) as avg_negative_score,

    sum(compound_score) as compound_score_sum,
    sum(abs(compound_score)) as absolute_sentiment_volume,
    sum(compound_score * confidence) / nullif(sum(confidence), 0) as confidence_weighted_compound_score,
    sum(compound_score * source_weight) / nullif(sum(source_weight), 0) as source_weighted_compound_score,
    avg(compound_score) * ln(1 + count(*)) as headline_volume_weighted_sentiment_index,

    min(published_at_utc) as first_headline_at_utc,
    max(published_at_utc) as latest_headline_at_utc

from scored

group by
    ticker,
    sentiment_date
