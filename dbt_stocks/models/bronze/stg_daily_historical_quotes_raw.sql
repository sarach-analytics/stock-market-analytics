-- models/bronze/stg_daily_historical_quotes_raw.sql
-- Bronze layer: Direct transformation from DAILY_HISTORICAL_QUOTES
-- Minimal cleaning, mostly just aliasing and data type casting
{{ config(
    materialized='table',
    database='STOCKS_MDS',
    schema='bronze',
    description='Raw daily historical quote data with minimal transformations'
) }}
SELECT
    -- Keys
    symbol,
    date as quote_date,
    
    -- Price data
    open_price,
    high_price,
    low_price,
    close_price,
    
    -- Volume and timestamp
    volume,
    unix_timestamp,
    
    -- Metadata
    data_source,
    fetched_at,
    load_timestamp,
    CURRENT_TIMESTAMP() as dbt_loaded_at,
    
    -- Add row number for deduplication
    ROW_NUMBER() OVER (
        PARTITION BY symbol, date 
        ORDER BY load_timestamp DESC
    ) as rn
FROM {{ source('stocks', 'DAILY_HISTORICAL_QUOTES') }}
WHERE 
    -- Filter out null values
    symbol IS NOT NULL
    AND date IS NOT NULL
    AND close_price IS NOT NULL