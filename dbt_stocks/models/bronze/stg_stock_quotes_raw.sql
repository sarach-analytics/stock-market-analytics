-- models/bronze/stg_stock_quotes_raw.sql
-- Bronze layer: Direct transformation from raw MinIO data
-- Minimal cleaning, mostly just aliasing and data type casting

{{ config(
    materialized='table',
    database='STOCKS_MDS',
    schema='bronze',
    description='Raw stock quote data from MinIO with minimal transformations'
) }}

SELECT
    -- Keys
    symbol,
    quote_timestamp,
    fetched_at,
    
    -- Price data
    open_price,
    high_price,
    low_price,
    close_price,
    prev_close,
    
    -- Change metrics
    price_change,
    price_change_percent,
    
    -- Metadata
    raw_json,
    load_timestamp,
    CURRENT_TIMESTAMP() as dbt_loaded_at,
    
    -- Add row number for deduplication in silver
    ROW_NUMBER() OVER (
        PARTITION BY symbol, quote_timestamp 
        ORDER BY load_timestamp DESC
    ) as rn

FROM {{ source('stocks', 'BRONZE_STOCK_QUOTES_RAW') }}

WHERE 
    -- Filter out null symbols
    symbol IS NOT NULL
    AND close_price IS NOT NULL
    AND quote_timestamp IS NOT NULL
