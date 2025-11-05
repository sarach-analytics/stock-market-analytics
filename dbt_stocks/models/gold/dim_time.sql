-- models/gold/dim_time.sql
-- Gold layer: Time dimension for time-based analysis
{{ config(
    materialized='table',
    database='STOCKS_MDS',
    schema='gold',
    description='Time dimension table for intraday time-based analysis'
) }}
WITH time_series AS (
    SELECT
        SEQ4() as minute_number
    FROM TABLE(GENERATOR(ROWCOUNT => 1440))  -- 24 hours * 60 minutes
),
time_values AS (
    SELECT
        minute_number,
        TIMEADD('minute', minute_number, '00:00:00'::TIME) as time_and_day,
        LPAD(EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)), 2, '0') || 
        LPAD(EXTRACT(MINUTE FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)), 2, '0') as time_key,
        TO_VARCHAR(TIMEADD('minute', minute_number, '00:00:00'::TIME), 'HH:MI AM/PM') as actual_time,
        EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME))::INTEGER as hour,
        CASE 
            WHEN EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) = 0 THEN '12 AM'
            WHEN EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) < 12 THEN 
                EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME))::VARCHAR || ' AM'
            WHEN EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) = 12 THEN '12 PM'
            ELSE (EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) - 12)::VARCHAR || ' PM'
        END as hour_extended,
        EXTRACT(MINUTE FROM TIMEADD('minute', minute_number, '00:00:00'::TIME))::INTEGER as minute,
        CASE 
            WHEN EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) < 12 THEN 'AM'
            ELSE 'PM'
        END as ampm,
        CASE 
            WHEN EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) BETWEEN 4 AND 8 THEN 'Pre-Market'
            WHEN EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) = 9
                AND EXTRACT(MINUTE FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) < 30
                THEN 'Pre-Market'
            WHEN EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) = 9
                AND EXTRACT(MINUTE FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) >= 30
                THEN 'Regular Hours'
            WHEN EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) BETWEEN 10 AND 15
                THEN 'Regular Hours'
            WHEN EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) = 16
                AND EXTRACT(MINUTE FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) = 0
                THEN 'Regular Hours'
            WHEN EXTRACT(HOUR FROM TIMEADD('minute', minute_number, '00:00:00'::TIME)) BETWEEN 16 AND 20 THEN 'Post-Market'
            ELSE 'Closed'
        END as market_period,
        CURRENT_TIMESTAMP() as dbt_loaded_at
    FROM time_series
)
SELECT
    minute_number,
    time_and_day,
    time_key,
    actual_time,
    hour,
    hour_extended,
    minute,
    ampm,
    market_period,
    CASE 
        WHEN market_period = 'Regular Hours' THEN 1
        ELSE 0
    END as is_market_open,
    dbt_loaded_at
FROM time_values
ORDER BY minute_number