-- models/gold/dim_date.sql
-- Gold layer: Date dimension for time-based analysis
{{ config(
    materialized='table',
    database='STOCKS_MDS',
    schema='gold',
    description='Date dimension table for time-based analysis'
) }}
WITH date_spine AS (
    SELECT 
        DATEADD(
            'day', 
            SEQ4(), 
            '2025-01-01'::DATE
        ) as date_key
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
)
SELECT
    date_key,
    YEAR(date_key) as year,
    MONTH(date_key) as month,
    MONTHNAME(date_key) as month_name,
    QUARTER(date_key) as quarter,
    DAYOFWEEK(date_key) as day_of_week,
    DAYNAME(date_key) as day_name,
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM date_spine
WHERE date_key <= (select max(quote_date) FROM {{ ref('stg_historical_quotes_cleaned') }})
ORDER BY date_key