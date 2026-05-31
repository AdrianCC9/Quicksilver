{{ config(materialized='view') }}

with scored as (
    select
        ticker,
        published_at_utc,
        cast(published_at_utc as date) as sentiment_date
    from {{ source('quicksilver', 'scored_headlines') }}
),

summary as (
    select
        min(published_at_utc) as first_scored_headline_at_utc,
        max(published_at_utc) as latest_scored_headline_at_utc,
        datediff('day', min(cast(published_at_utc as date)), max(cast(published_at_utc as date))) + 1
            as coverage_days,
        count(*) as total_scored_headlines,
        count(distinct ticker) as tracked_ticker_count,
        count(*) / nullif(
            datediff('day', min(cast(published_at_utc as date)), max(cast(published_at_utc as date))) + 1,
            0
        ) as avg_scored_headlines_per_day
    from scored
),

daily as (
    select
        sentiment_date,
        count(*) as scored_headlines
    from scored
    group by sentiment_date
)

select
    summary.first_scored_headline_at_utc,
    summary.latest_scored_headline_at_utc,
    summary.coverage_days,
    summary.total_scored_headlines,
    summary.tracked_ticker_count,
    summary.avg_scored_headlines_per_day,
    max(daily.scored_headlines) as max_scored_headlines_in_one_day,
    count_if(daily.scored_headlines >= 500) as days_with_500_plus_scored_headlines,
    summary.tracked_ticker_count >= 50 as has_50_plus_tickers,
    summary.coverage_days >= 730 as has_2_plus_years,
    max(daily.scored_headlines) >= 500 as has_500_plus_daily_headlines
from summary
left join daily on true
group by
    summary.first_scored_headline_at_utc,
    summary.latest_scored_headline_at_utc,
    summary.coverage_days,
    summary.total_scored_headlines,
    summary.tracked_ticker_count,
    summary.avg_scored_headlines_per_day
