# dags/backfill_ytd_historical.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import yfinance as yf
import json
import boto3
import pandas as pd
import snowflake.connector
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION FUNCTIONS
# ============================================

def get_minio_config() -> Dict:
    try:
        conn = BaseHook.get_connection('minio_s3')
        return {
            'endpoint_url': json.loads(conn.extra).get('endpoint_url', 'http://minio:9000'),
            'aws_access_key_id': conn.login,
            'aws_secret_access_key': conn.password,
            'bucket': Variable.get('minio_bucket', default_var='bronze-transactions')
        }
    except Exception as e:
        logger.warning(f"Could not get MinIO connection: {e}. Using variables.")
        return {
            'endpoint_url': Variable.get('minio_endpoint', default_var='http://minio:9000'),
            'aws_access_key_id': Variable.get('minio_access_key'),
            'aws_secret_access_key': Variable.get('minio_secret_key'),
            'bucket': Variable.get('minio_bucket', default_var='bronze-transactions')
        }

def get_snowflake_config() -> Dict:
    try:
        conn = BaseHook.get_connection('snowflake_default')
        extra = json.loads(conn.extra) if conn.extra else {}
        
        return {
            'user': conn.login,
            'password': conn.password,
            'account': extra.get('account'),
            'warehouse': extra.get('warehouse', 'COMPUTE_WH'),
            'database': extra.get('database', 'STOCKS_MDS'),
            'schema': conn.schema or 'COMMON',
            'role': extra.get('role', 'DATA_PIPELINE_ROLE')
        }
    except Exception as e:
        logger.error(f"Failed to get Snowflake connection: {e}")
        raise

# ============================================
# CONFIGURATION
# ============================================

SYMBOLS = ["AMZN", "PANW", "NVDA", "AMD", "TSM","AVGO"]
START_DATE = "2025-01-01"
END_DATE = datetime.now().strftime('%Y-%m-%d')

# ============================================
# SETUP SNOWFLAKE TABLE
# ============================================

def setup_snowflake_table():
    """Create YTD historical table in Snowflake"""
    
    sf_config = get_snowflake_config()
    
    try:
        conn = snowflake.connector.connect(
            user=sf_config['user'],
            password=sf_config['password'],
            account=sf_config['account'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            schema=sf_config['schema'],
            role=sf_config['role']
        )
        
        cur = conn.cursor()
        
        # Create YTD historical table
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {sf_config['database']}.{sf_config['schema']}.YTD_HISTORICAL_QUOTES (
            SYMBOL VARCHAR(10) NOT NULL,
            DATE DATE NOT NULL,
            OPEN_PRICE DECIMAL(10, 2),
            HIGH_PRICE DECIMAL(10, 2),
            LOW_PRICE DECIMAL(10, 2),
            CLOSE_PRICE DECIMAL(10, 2),
            VOLUME INTEGER,
            UNIX_TIMESTAMP INTEGER,
            FETCHED_AT TIMESTAMP_NTZ,
            DATA_SOURCE VARCHAR(50),
            LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (SYMBOL, DATE)
        )
        """
        
        cur.execute(create_table_sql)
        logger.info(f"✓ Snowflake YTD table ready: {sf_config['database']}.{sf_config['schema']}.YTD_HISTORICAL_QUOTES")
        
        cur.close()
        conn.close()
    
    except Exception as e:
        logger.error(f"Error setting up Snowflake table: {e}")
        raise

# ============================================
# FETCH AND UPLOAD YTD DATA
# ============================================

def fetch_and_upload_ytd_data():
    """Fetch all YTD data and upload to MinIO"""
    
    minio_config = get_minio_config()
    
    logger.info(f"Fetching YTD data from {START_DATE} to {END_DATE}")
    
    # MinIO connection
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_config['endpoint_url'],
        aws_access_key_id=minio_config['aws_access_key_id'],
        aws_secret_access_key=minio_config['aws_secret_access_key']
    )
    
    total_records = 0
    failed_symbols = []
    
    for symbol in SYMBOLS:
        try:
            logger.info(f"Fetching {symbol} YTD data ({START_DATE} to {END_DATE})...")
            
            # Download historical data
            data = yf.download(
                symbol,
                start=START_DATE,
                end=END_DATE,
                progress=False
            )
            
            if data.empty:
                logger.warning(f"No data returned for {symbol}")
                failed_symbols.append(symbol)
                continue
            
            logger.info(f"  Downloaded {len(data)} trading days for {symbol}")
            
            # Process each trading day
            for date, row in data.iterrows():
                try:
                    # Create record
                    record = {
                        "symbol": symbol,
                        "date": date.strftime('%Y-%m-%d'),
                        "o": round(float(row['Open']), 2),
                        "h": round(float(row['High']), 2),
                        "l": round(float(row['Low']), 2),
                        "c": round(float(row['Close']), 2),
                        "v": int(row['Volume']) if 'Volume' in row else 0,
                        "t": int(date.timestamp()),
                        "fetched_at": int(datetime.now().timestamp()),
                        "data_source": "yfinance"
                    }
                    
                    # Save to MinIO
                    date_str = date.strftime('%Y-%m-%d')
                    key = f"ytd_backfill/{symbol}/{date_str}.json"
                    
                    s3.put_object(
                        Bucket=minio_config['bucket'],
                        Key=key,
                        Body=json.dumps(record),
                        ContentType="application/json"
                    )
                    
                    total_records += 1
                    
                    # Log progress every 50 records
                    if total_records % 50 == 0:
                        logger.info(f"  ✓ Saved {total_records} records so far...")
                
                except Exception as e:
                    logger.error(f"  ✗ Error saving {symbol} {date_str}: {e}")
                    continue
            
            logger.info(f"✓ Successfully saved {len(data)} records for {symbol}")
        
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            failed_symbols.append(symbol)
            continue
    
    logger.info(f"\n✓ YTD backfill complete: {total_records} total records saved")
    if failed_symbols:
        logger.warning(f"Failed symbols: {', '.join(failed_symbols)}")
    
    return total_records

# ============================================
# LOAD YTD DATA TO SNOWFLAKE
# ============================================

def load_ytd_to_snowflake():
    """Load all YTD data from MinIO to Snowflake"""
    
    minio_config = get_minio_config()
    sf_config = get_snowflake_config()
    
    try:
        conn = snowflake.connector.connect(
            user=sf_config['user'],
            password=sf_config['password'],
            account=sf_config['account'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            schema=sf_config['schema'],
            role=sf_config['role']
        )
        
        cur = conn.cursor()
        
        # MinIO connection to read files
        s3 = boto3.client(
            "s3",
            endpoint_url=minio_config['endpoint_url'],
            aws_access_key_id=minio_config['aws_access_key_id'],
            aws_secret_access_key=minio_config['aws_secret_access_key']
        )
        
        records_loaded = 0
        
        # List all files in ytd_backfill directory
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=minio_config['bucket'], Prefix='ytd_backfill/')
        
        for page in pages:
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                try:
                    key = obj['Key']
                    
                    # Skip directory markers
                    if key.endswith('/'):
                        continue
                    
                    # Read file from MinIO
                    response = s3.get_object(Bucket=minio_config['bucket'], Key=key)
                    data = json.loads(response['Body'].read().decode('utf-8'))
                    
                    # Insert into Snowflake
                    insert_sql = f"""
                    INSERT INTO {sf_config['database']}.{sf_config['schema']}.YTD_HISTORICAL_QUOTES
                    (SYMBOL, DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, VOLUME, UNIX_TIMESTAMP, FETCHED_AT, DATA_SOURCE)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    cur.execute(insert_sql, (
                        data['symbol'],
                        data['date'],
                        data['o'],
                        data['h'],
                        data['l'],
                        data['c'],
                        data['v'],
                        data['t'],
                        datetime.fromtimestamp(data['fetched_at']),
                        data['data_source']
                    ))
                    
                    records_loaded += 1
                    
                    if records_loaded % 100 == 0:
                        logger.info(f"  ✓ Loaded {records_loaded} records...")
                
                except Exception as e:
                    logger.error(f"Error loading {key}: {e}")
                    continue
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"✓ YTD data loaded to Snowflake successfully ({records_loaded} records)")
    
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        raise

# ============================================
# DAG DEFINITION
# ============================================

default_args = {
    'owner': 'data_engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['your_email@example.com'],
}

with DAG(
    'backfill_ytd_historical',
    default_args=default_args,
    description='One-time backfill of YTD historical data from Jan 1 to today',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['stocks', 'backfill', 'historical'],
) as dag:
    
    setup_task = PythonOperator(
        task_id='setup_snowflake_table',
        python_callable=setup_snowflake_table,
    )
    
    fetch_task = PythonOperator(
        task_id='fetch_ytd_data',
        python_callable=fetch_and_upload_ytd_data,
    )
    
    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_ytd_to_snowflake,
    )
    
    setup_task >> fetch_task >> load_task