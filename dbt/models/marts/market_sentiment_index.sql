{{ config(materialized='view') }}

with daily as (
    select
        sentiment_date,
        ticker,
        headline_count,
        avg_compound_score,
        confidence_weighted_compound_score,
        source_weighted_compound_score
    from {{ ref('ticker_sentiment_daily') }}
),

indexed as (
    select
        sentiment_date,
        count(distinct ticker) as ticker_count,
        sum(headline_count) as headline_count,
        avg(avg_compound_score) as equal_weight_sentiment_index,

        sum(avg_compound_score * headline_count)
            / nullif(sum(headline_count), 0) as volume_weighted_sentiment_index,

        sum(confidence_weighted_compound_score * headline_count)
            / nullif(sum(headline_count), 0) as confidence_volume_weighted_sentiment_index,

        sum(source_weighted_compound_score * headline_count)
            / nullif(sum(headline_count), 0) as source_volume_weighted_sentiment_index
    from daily
    group by sentiment_date
)

select
    sentiment_date,
    ticker_count,
    headline_count,
    equal_weight_sentiment_index,
    volume_weighted_sentiment_index,
    confidence_volume_weighted_sentiment_index,
    source_volume_weighted_sentiment_index,

    avg(volume_weighted_sentiment_index) over (
        order by sentiment_date
        rows between 6 preceding and current row
    ) as rolling_7_day_volume_weighted_sentiment_index,

    stddev_samp(volume_weighted_sentiment_index) over (
        order by sentiment_date
        rows between 6 preceding and current row
    ) as rolling_7_day_volume_weighted_sentiment_stddev

from indexed
