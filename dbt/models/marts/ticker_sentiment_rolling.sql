{{ config(materialized='view') }}

select
    ticker,
    sentiment_date,
    headline_count,
    avg_compound_score,
    avg_confidence,
    positive_headline_count,
    neutral_headline_count,
    negative_headline_count,
    confidence_weighted_compound_score,
    source_weighted_compound_score,
    headline_volume_weighted_sentiment_index,

    avg(avg_compound_score) over (
        partition by ticker
        order by sentiment_date
        rows between 6 preceding and current row
    ) as rolling_7_day_avg_compound_score,

    avg(headline_count) over (
        partition by ticker
        order by sentiment_date
        rows between 6 preceding and current row
    ) as rolling_7_day_avg_headline_count,

    stddev_samp(avg_compound_score) over (
        partition by ticker
        order by sentiment_date
        rows between 6 preceding and current row
    ) as rolling_7_day_compound_score_stddev,

    sum(avg_compound_score * headline_count) over (
        partition by ticker
        order by sentiment_date
        rows between 6 preceding and current row
    ) / nullif(
        sum(headline_count) over (
            partition by ticker
            order by sentiment_date
            rows between 6 preceding and current row
        ),
        0
    ) as rolling_7_day_volume_weighted_sentiment_index,

    sum(compound_score_sum) over (
        partition by ticker
        order by sentiment_date
        rows between 6 preceding and current row
    ) as rolling_7_day_compound_score_sum,

    sum(absolute_sentiment_volume) over (
        partition by ticker
        order by sentiment_date
        rows between 6 preceding and current row
    ) as rolling_7_day_absolute_sentiment_volume

from {{ ref('ticker_sentiment_daily') }}
