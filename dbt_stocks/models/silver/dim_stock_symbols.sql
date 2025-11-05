-- models/silver/dim_stock_symbols.sql
-- Silver layer: Dimension table of stock symbols with metadata
{{ config(
    materialized='table',
    database='STOCKS_MDS',
    schema='silver',
    description='Dimension table of stock symbols with first and last quote dates'
) }}
WITH all_symbols AS (
    SELECT DISTINCT symbol FROM {{ ref('stg_stock_quotes_cleaned') }}
    UNION ALL
    SELECT DISTINCT symbol FROM {{ ref('stg_historical_quotes_cleaned') }}
)
SELECT
    symbol,
    MIN(quote_date) as first_quote_date,
    MAX(quote_date) as last_quote_date,
    COUNT(*) as total_quotes,
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM (
    SELECT symbol, quote_date FROM {{ ref('stg_stock_quotes_cleaned') }}
    UNION ALL
    SELECT symbol, quote_date FROM {{ ref('stg_historical_quotes_cleaned') }}
)
GROUP BY symbol