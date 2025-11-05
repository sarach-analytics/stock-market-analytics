-- models/silver/stg_historical_quotes_cleaned.sql
-- Silver layer: Combine and clean daily and YTD historical quotes
{{ config(
    materialized='table',
    database='STOCKS_MDS',
    schema='silver',
    description='Combined and cleaned historical quotes from daily and YTD sources'
) }}
WITH daily_deduplicated AS (
    SELECT * 
    FROM {{ ref('stg_daily_historical_quotes_raw') }}
    WHERE rn = 1
),
ytd_deduplicated AS (
    SELECT * 
    FROM {{ ref('stg_ytd_historical_quotes_raw') }}
    WHERE rn = 1
),
combined AS (
    SELECT
        symbol,
        quote_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        unix_timestamp,
        data_source,
        fetched_at,
        load_timestamp,
        dbt_loaded_at
    FROM daily_deduplicated
    
    UNION ALL
    
    SELECT
        symbol,
        quote_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        unix_timestamp,
        data_source,
        fetched_at,
        load_timestamp,
        dbt_loaded_at
    FROM ytd_deduplicated
),
deduplicated_combined AS (
    SELECT
        symbol,
        quote_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        unix_timestamp,
        data_source,
        fetched_at,
        load_timestamp,
        dbt_loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, quote_date
            ORDER BY load_timestamp DESC
        ) as rn
    FROM combined
),
validated AS (
    SELECT
        symbol,
        quote_date,
        
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
        
        CAST(volume AS NUMBER(38,0)) as volume,
        unix_timestamp,
        data_source,
        fetched_at,
        load_timestamp,
        dbt_loaded_at,
        
        CASE 
            WHEN close_price IS NULL OR close_price <= 0 THEN 'INVALID'
            WHEN high_price < low_price THEN 'INCONSISTENT'
            WHEN close_price > high_price OR close_price < low_price THEN 'INCONSISTENT'
            ELSE 'VALID'
        END as data_quality_flag
    FROM deduplicated_combined
    WHERE rn = 1
)
SELECT * FROM validated