-- models/gold/fact_stock_performance.sql
-- Gold layer: Stock performance with technical indicators
{{ config(
    materialized='table',
    database='STOCKS_MDS',
    schema='gold',
    description='Stock performance with moving averages and technical indicators'
) }}
WITH historical_data AS (
    SELECT
        symbol,
        quote_date,
        close_price_validated as close_price
    FROM {{ ref('stg_historical_quotes_cleaned') }}
    WHERE data_quality_flag = 'VALID'
    ORDER BY symbol, quote_date
),
with_moving_averages AS (
    SELECT
        symbol,
        quote_date,
        close_price,
        
        -- Moving averages
        AVG(close_price) OVER (
            PARTITION BY symbol 
            ORDER BY quote_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as ma_7day,
        
        AVG(close_price) OVER (
            PARTITION BY symbol 
            ORDER BY quote_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as ma_30day,
        
        AVG(close_price) OVER (
            PARTITION BY symbol 
            ORDER BY quote_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as ma_90day,
        
        FIRST_VALUE(close_price) OVER (
            PARTITION BY symbol, YEAR(quote_date)
            ORDER BY quote_date
        ) as ytd_open_price
    FROM historical_data
),
with_returns AS (
    SELECT
        symbol,
        quote_date,
        close_price,
        ma_7day,
        ma_30day,
        ma_90day,
        
        -- YTD return
        ROUND(
            ((close_price - ytd_open_price) / ytd_open_price) * 100, 
            2
        ) as ytd_return_percent,
        
        -- 30-day momentum
        LAG(close_price, 30) OVER (
            PARTITION BY symbol 
            ORDER BY quote_date
        ) as price_30days_ago
    FROM with_moving_averages
)
SELECT
    symbol,
    quote_date,
    ROUND(close_price, 2) as close_price,
    ROUND(ma_7day, 2) as ma_7day,
    ROUND(ma_30day, 2) as ma_30day,
    ROUND(ma_90day, 2) as ma_90day,
    ytd_return_percent,
    ROUND(
        ((close_price - price_30days_ago) / price_30days_ago) * 100,
        2
    ) as momentum_30day_percent,
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM with_returns