{{ config(materialized='view') }}

select
    ticker,
    sentiment_date,
    headline_count,
    avg_compound_score,
    rolling_7_day_avg_compound_score,
    rolling_7_day_avg_headline_count,
    rolling_7_day_compound_score_stddev,

    case
        when avg_compound_score <= -0.50 then true
        else false
    end as is_negative_sentiment_signal,

    case
        when avg_compound_score >= 0.50 then true
        else false
    end as is_positive_sentiment_signal,

    case
        when rolling_7_day_compound_score_stddev is null
        or rolling_7_day_compound_score_stddev = 0
        then null
        else
            (avg_compound_score - rolling_7_day_avg_compound_score)
            / rolling_7_day_compound_score_stddev
    end as compound_score_zscore

from {{ ref('ticker_sentiment_rolling') }}
