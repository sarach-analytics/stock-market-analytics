-- models/silver/stg_stock_quotes_cleaned.sql
-- Silver layer: Clean, validate and standardize real-time stock quotes
{{ config(
    materialized='table',
    database='STOCKS_MDS',
    schema='silver',
    description='Cleaned and validated real-time stock quotes with data quality flags'
) }}
WITH deduplicated AS (
    SELECT * 
    FROM {{ ref('stg_stock_quotes_raw') }}
    WHERE rn = 1
),
validated AS (
    SELECT
        symbol,
        CONVERT_TIMEZONE('UTC', 'America/New_York', quote_timestamp) as quote_date,
        CONVERT_TIMEZONE('UTC', 'America/New_York', quote_timestamp) as quote_timestamp,
        -- CAST(quote_timestamp AS TIMESTAMP_NTZ(9)) as quote_date,
        -- quote_timestamp,
        fetched_at,
        
        -- Price validation
        CAST(CASE 
            WHEN open_price <= 0 OR open_price IS NULL THEN NULL
            ELSE open_price 
        END AS NUMBER(10,2)) as open_price_validated,
        
        CAST(CASE 
            WHEN high_price <= 0 OR high_price IS NULL THEN NULL
            ELSE high_price 
        END AS NUMBER(10,2)) as high_price_validated,
        
        CAST(CASE 
            WHEN low_price <= 0 OR low_price IS NULL THEN NULL
            ELSE low_price 
        END AS NUMBER(10,2)) as low_price_validated,
        
        CAST(CASE 
            WHEN close_price <= 0 OR close_price IS NULL THEN NULL
            ELSE close_price 
        END AS NUMBER(10,2)) as close_price_validated,
        
        CAST(prev_close AS NUMBER(10,2)) as prev_close_validated,
        CAST(price_change AS NUMBER(10,4)) as price_change_validated,
        CAST(price_change_percent AS NUMBER(10,4)) as price_change_percent_validated,
        
        -- Data quality flag
        CASE 
            WHEN close_price IS NULL OR close_price <= 0 THEN 'INVALID'
            WHEN high_price < low_price THEN 'INCONSISTENT'
            WHEN close_price > high_price OR close_price < low_price THEN 'INCONSISTENT'
            WHEN ABS(price_change_percent) > 50 THEN 'OUTLIER'
            ELSE 'VALID'
        END as data_quality_flag,
        
        load_timestamp,
        dbt_loaded_at
    FROM deduplicated
)
SELECT * FROM validated