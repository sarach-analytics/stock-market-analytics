-- models/gold/fact_daily_stock_quotes.sql
-- Gold layer: Daily stock facts for Power BI real-time dashboard
{{ config(
    materialized='table',
    database='STOCKS_MDS',
    schema='gold',
    description='Daily stock quote facts optimized for Power BI real-time dashboard'
) }}
WITH latest_quotes AS (
    SELECT
        symbol,
        quote_date,
        LPAD(EXTRACT(HOUR FROM quote_date), 2, '0') || 
        LPAD(EXTRACT(MINUTE FROM quote_date), 2, '0') as time_key,
        open_price_validated as open_price,
        high_price_validated as high_price,
        low_price_validated as low_price,
        close_price_validated as close_price,
        price_change_percent_validated as price_change_percent,
        data_quality_flag
    FROM {{ ref('stg_stock_quotes_cleaned') }}
    WHERE data_quality_flag = 'VALID'
)
SELECT
    symbol,
    quote_date,
    time_key,
    open_price,
    high_price,
    low_price,
    close_price,
    ROUND(price_change_percent, 2) as daily_return_percent,
    
    -- Volatility categorization
    CASE
        WHEN ABS(price_change_percent) < 1 THEN 'LOW'
        WHEN ABS(price_change_percent) BETWEEN 1 AND 3 THEN 'MEDIUM'
        ELSE 'HIGH'
    END as volatility_category,
    
    -- Price direction
    CASE
        WHEN close_price > open_price THEN 'UP'
        WHEN close_price < open_price THEN 'DOWN'
        ELSE 'FLAT'
    END as price_direction,
    
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM latest_quotes