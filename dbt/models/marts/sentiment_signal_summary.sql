{{ config(materialized='view') }}

with scored as (
    select
        ticker,
        sentiment_date,
        headline_count,
        avg_compound_score,
        confidence_weighted_compound_score,
        source_weighted_compound_score,
        headline_volume_weighted_sentiment_index,
        rolling_7_day_avg_compound_score,
        rolling_7_day_avg_headline_count,
        rolling_7_day_compound_score_stddev,
        rolling_7_day_volume_weighted_sentiment_index,
        rolling_7_day_compound_score_sum,
        rolling_7_day_absolute_sentiment_volume,

        case
            when rolling_7_day_compound_score_stddev is null
            or rolling_7_day_compound_score_stddev = 0
            then null
            else
                (avg_compound_score - rolling_7_day_avg_compound_score)
                / rolling_7_day_compound_score_stddev
        end as compound_score_zscore

    from {{ ref('ticker_sentiment_rolling') }}
)

select
    ticker,
    sentiment_date,
    headline_count,
    avg_compound_score,
    confidence_weighted_compound_score,
    source_weighted_compound_score,
    headline_volume_weighted_sentiment_index,
    rolling_7_day_avg_compound_score,
    rolling_7_day_avg_headline_count,
    rolling_7_day_compound_score_stddev,
    rolling_7_day_volume_weighted_sentiment_index,
    rolling_7_day_compound_score_sum,
    rolling_7_day_absolute_sentiment_volume,
    compound_score_zscore,

    case
        when avg_compound_score <= -{{ var('sentiment_score_threshold') }}
        or compound_score_zscore <= -{{ var('sentiment_zscore_threshold') }}
        then true
        else false
    end as is_negative_sentiment_signal,

    case
        when avg_compound_score >= {{ var('sentiment_score_threshold') }}
        or compound_score_zscore >= {{ var('sentiment_zscore_threshold') }}
        then true
        else false
    end as is_positive_sentiment_signal,

    case
        when compound_score_zscore <= -{{ var('sentiment_zscore_threshold') }}
        then true
        else false
    end as is_negative_zscore_anomaly,

    case
        when compound_score_zscore >= {{ var('sentiment_zscore_threshold') }}
        then true
        else false
    end as is_positive_zscore_anomaly,

    case
        when abs(compound_score_zscore) >= {{ var('sentiment_zscore_threshold') }}
        then true
        else false
    end as is_zscore_anomaly

from scored
