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
    ) as rolling_7_day_compound_score_stddev

from {{ ref('ticker_sentiment_daily') }}
