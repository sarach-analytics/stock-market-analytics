# dags/daily_historical_load.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import yfinance as yf
import json
import boto3
import pandas as pd
import snowflake.connector
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Slack webhook URL - Store in Airflow Variable for security
SLACK_WEBHOOK_URL = Variable.get("slack_webhook_url", default_var=None)

SYMBOLS = ["AMZN", "PANW", "NVDA", "AMD", "TSM", "AVGO"]

# ============================================
# SLACK NOTIFICATION FUNCTIONS
# ============================================

def send_slack_alert(message: str, severity: str = 'info', context: dict = None):
    """Send simplified alert to Slack"""
    if not SLACK_WEBHOOK_URL:
        logger.warning("Slack webhook URL not configured. Skipping alert.")
        return

    colors = {'info': '#36a64f', 'warning': '#ff9900', 'critical': '#ff0000'}
    emojis = {'info': '✅', 'warning': '⚠️', 'critical': '❌'}

    fields = []
    if context and 'task_instance' in context:
        ti = context['task_instance']
        fields = [
            {"title": "Task", "value": ti.task_id, "short": True},
            {"title": "DAG", "value": ti.dag_id, "short": True}
        ]

    payload = {
        "attachments": [{
            "color": colors.get(severity, '#36a64f'),
            "title": f"{emojis.get(severity, '📢')} Daily Pipeline - {severity.upper()}",
            "text": message,
            "fields": fields,
            "ts": int(datetime.utcnow().timestamp())
        }]
    }

    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        if response.status_code != 200:
            logger.error(f"Slack notification failed: {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to send Slack alert: {e}")


def task_failure_callback(context):
    """Called when any task fails"""
    ti = context.get('task_instance')
    exception = context.get('exception')

    message = f"Task `{ti.task_id}` failed\nError: {str(exception)[:200]}"
    send_slack_alert(message, severity='critical', context=context)


def task_success_callback(context):
    """Called when pipeline completes successfully"""
    ti = context['ti']
    records_loaded = ti.xcom_pull(task_ids='load_to_snowflake', key='records_loaded') or 0
    records_updated = ti.xcom_pull(task_ids='load_to_snowflake', key='records_updated') or 0

    message = f"Pipeline completed\nInserted: {records_loaded} | Updated: {records_updated}"
    send_slack_alert(message, severity='info', context=context)

# ============================================
# CONFIGURATION FUNCTIONS
# ============================================

def get_minio_config():
    """Get MinIO configuration from Airflow Connections/Variables."""
    try:
        conn = BaseHook.get_connection('minio_s3')
        return {
            'endpoint_url': json.loads(conn.extra).get('endpoint_url', 'http://minio:9000'),
            'aws_access_key_id': conn.login,
            'aws_secret_access_key': conn.password,
            'bucket': Variable.get('minio_bucket', default_var='bronze-transactions')
        }
    except Exception as e:
        logger.warning(f"Could not get MinIO connection: {e}")
        return {
            'endpoint_url': Variable.get('minio_endpoint', default_var='http://minio:9000'),
            'aws_access_key_id': Variable.get('minio_access_key'),
            'aws_secret_access_key': Variable.get('minio_secret_key'),
            'bucket': Variable.get('minio_bucket', default_var='bronze-transactions')
        }

def get_snowflake_config():
    """Get Snowflake configuration from Airflow Connection."""
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

# ============================================
# HEALTH CHECK TASKS
# ============================================

def check_minio_connectivity(**kwargs):
    """Check if MinIO is reachable"""
    try:
        config = get_minio_config()
        s3 = boto3.client(
            "s3",
            endpoint_url=config['endpoint_url'],
            aws_access_key_id=config['aws_access_key_id'],
            aws_secret_access_key=config['aws_secret_access_key']
        )
        
        s3.list_buckets()
        logger.info("✓ MinIO connectivity check passed")
        
    except Exception as e:
        error_msg = f"""
        **MINIO UNREACHABLE - Daily Pipeline**
        
        Error: {str(e)}
        Endpoint: {config.get('endpoint_url', 'unknown')}
        
        **Impact**: Cannot write historical data to MinIO
        **Action**: Check MinIO service status
        """
        send_slack_alert(error_msg, severity='critical', context=kwargs)
        raise

def check_snowflake_connectivity(**kwargs):
    """Check if Snowflake is reachable"""
    try:
        config = get_snowflake_config()
        conn = snowflake.connector.connect(
            user=config['user'],
            password=config['password'],
            account=config['account'],
            warehouse=config['warehouse'],
            database=config['database']
        )
        conn.close()
        logger.info("✓ Snowflake connectivity check passed")
        
    except Exception as e:
        error_msg = f"""
        **SNOWFLAKE UNREACHABLE - Daily Pipeline**
        
        Error: {str(e)}
        Account: {config.get('account', 'unknown')}
        
        **Impact**: Cannot load historical data to Snowflake
        **Action**: Check Snowflake credentials and network
        """
        send_slack_alert(error_msg, severity='critical', context=kwargs)
        raise

def check_yfinance_api(**kwargs):
    """Check if yfinance API is working"""
    try:
        # Try to fetch a small sample
        test_data = yf.download(
            "AAPL",
            start=(datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d'),
            end=datetime.now().strftime('%Y-%m-%d'),
            progress=False,
            timeout=10
        )
        
        if test_data.empty:
            raise Exception("yfinance returned empty data")
        
        logger.info("✓ yfinance API check passed")
        
    except Exception as e:
        error_msg = f"""
        **YFINANCE API UNAVAILABLE**
        
        Error: {str(e)}
        
        **Impact**: Cannot fetch historical stock data
        **Action**: 
        - Check yfinance API status
        - Check network connectivity
        - May need to wait and retry later
        """
        send_slack_alert(error_msg, severity='critical', context=kwargs)
        raise

# ============================================
# SETUP SNOWFLAKE TABLE
# ============================================

def setup_snowflake_table(**kwargs):
    """Create table in Snowflake if it doesn't exist"""
    try:
        sf_config = get_snowflake_config()
        
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
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {sf_config['database']}.{sf_config['schema']}.DAILY_HISTORICAL_QUOTES (
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
        logger.info(f"✓ Snowflake table ready")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        error_msg = f"""
        **FAILED TO SETUP SNOWFLAKE TABLE**
        
        Error: {str(e)}
        
        **Action**: Check Snowflake permissions and table schema
        """
        send_slack_alert(error_msg, severity='critical', context=kwargs)
        raise

# ============================================
# FETCH AND UPLOAD DAILY DATA
# ============================================

def fetch_and_upload_daily_data(**kwargs):
    """Fetch data from yfinance and upload to MinIO with enhanced error tracking"""
    minio_config = get_minio_config()
    
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    logger.info(f"Target date: {target_date}")
    logger.info(f"MinIO config - endpoint: {minio_config['endpoint_url']}, bucket: {minio_config['bucket']}")
    
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_config['endpoint_url'],
        aws_access_key_id=minio_config['aws_access_key_id'],
        aws_secret_access_key=minio_config['aws_secret_access_key']
    )
    
    records_uploaded = 0
    failed_symbols = []
    successful_symbols = []
    fetch_errors = {}  # Track specific errors per symbol
    
    for symbol in SYMBOLS:
        logger.info(f"Fetching {symbol}...")
        
        end_date = (pd.Timestamp(target_date) + timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (pd.Timestamp(target_date) - timedelta(days=30)).strftime('%Y-%m-%d')
        
        logger.info(f"Date range: {start_date} to {end_date}")
        
        try:
            data = yf.download(
                symbol,
                start=start_date,
                end=end_date,
                progress=False,
                auto_adjust=False,
                threads=False,
                timeout=10
            )
        except Exception as e:
            error_detail = str(e)[:100]
            fetch_errors[symbol] = error_detail
            logger.error(f"Error fetching {symbol}: {e}", exc_info=True)
            failed_symbols.append(symbol)
            continue
        
        logger.info(f"Downloaded {len(data)} rows for {symbol}")
        
        if data.empty:
            fetch_errors[symbol] = "No data available"
            logger.warning(f"No data available for {symbol}")
            failed_symbols.append(symbol)
            continue
        
        try:
            row = data.iloc[-1]
            actual_date = data.index[-1].strftime('%Y-%m-%d')
            
            close_val = float(row['Close'])
            open_val = float(row['Open'])
            high_val = float(row['High'])
            low_val = float(row['Low'])
            vol_val = int(row['Volume'])
            
            if pd.isna(close_val) or pd.isna(open_val):
                fetch_errors[symbol] = "Invalid/missing OHLC data"
                logger.warning(f"Invalid data for {symbol}")
                failed_symbols.append(symbol)
                continue
            
            timestamp = int(pd.Timestamp(actual_date).timestamp())
            
            record = {
                "symbol": symbol,
                "date": actual_date,
                "o": round(open_val, 2),
                "h": round(high_val, 2),
                "l": round(low_val, 2),
                "c": round(close_val, 2),
                "v": vol_val,
                "t": timestamp,
                "fetched_at": int(datetime.now().timestamp()),
                "data_source": "yfinance"
            }
            
            logger.info(f"Record for {symbol} on {actual_date}: O={record['o']}, H={record['h']}, L={record['l']}, C={record['c']}, V={record['v']}")
            
            key = f"daily/{symbol}/{actual_date}.json"
            s3.put_object(
                Bucket=minio_config['bucket'],
                Key=key,
                Body=json.dumps(record),
                ContentType="application/json"
            )
            
            records_uploaded += 1
            successful_symbols.append(symbol)
            logger.info(f"✓ Uploaded {symbol} to MinIO: {key}")
        
        except Exception as e:
            error_detail = str(e)[:100]
            fetch_errors[symbol] = error_detail
            logger.error(f"Error processing {symbol}: {e}", exc_info=True)
            failed_symbols.append(symbol)
            continue
    
    logger.info(f"Daily upload complete: {records_uploaded} records uploaded")
    
    # Store results for next task
    kwargs['ti'].xcom_push(key='records_uploaded', value=records_uploaded)
    kwargs['ti'].xcom_push(key='failed_symbols', value=failed_symbols)
    kwargs['ti'].xcom_push(key='fetch_errors', value=fetch_errors)
    
    # Determine alert severity
    if records_uploaded == 0:
        severity = 'critical'
        error_details = "\n".join([f"  • {s}: {fetch_errors.get(s, 'Unknown')}" for s in failed_symbols])
        message = f"""
        **DAILY FETCH FAILED - NO DATA UPLOADED**
        
        Target date: {target_date}
        Total symbols: {len(SYMBOLS)}
        Failed: {len(failed_symbols)}
        
        Failed symbols:
        {error_details}
        
        **Action**: Check yFinance API and network connectivity
        """
    elif len(failed_symbols) > len(SYMBOLS) / 2:
        severity = 'warning'
        error_details = "\n".join([f"  • {s}: {fetch_errors.get(s, 'Unknown')}" for s in failed_symbols])
        message = f"""
        **DAILY FETCH PARTIAL FAILURE**
        
        Target date: {target_date}
        Successfully uploaded: {records_uploaded}/{len(SYMBOLS)}
        Failed: {len(failed_symbols)}
        
        Failed symbols:
        {error_details}
        
        **Action**: Review failed symbols and retry if needed
        """
    elif failed_symbols:
        severity = 'warning'
        error_details = "\n".join([f"  • {s}: {fetch_errors.get(s, 'Unknown')}" for s in failed_symbols])
        message = f"""
        **DAILY FETCH COMPLETED WITH SOME FAILURES**
        
        Target date: {target_date}
        Successfully uploaded: {records_uploaded}/{len(SYMBOLS)}
        Failed: {len(failed_symbols)}
        
        Failed symbols:
        {error_details}
        """
    else:
        severity = 'info'
        message = f"""
        **DAILY FETCH SUCCESS** ✅
        
        Target date: {target_date}
        All {records_uploaded} symbols uploaded successfully
        Symbols: {', '.join(successful_symbols)}
        """
    
    send_slack_alert(
        message=message,
        severity=severity,
        context=kwargs,
        additional_context={
            'symbols_processed': records_uploaded,
            'symbols_failed': len(failed_symbols),
            'target_date': target_date,
            'data_source': 'yFinance'
        }
    )
    
    if failed_symbols:
        logger.warning(f"Failed symbols: {', '.join(failed_symbols)}")
    
    return records_uploaded

# ============================================
# LOAD TO SNOWFLAKE
# ============================================

def load_daily_to_snowflake(**kwargs):
    """Load data from MinIO to Snowflake with enhanced error tracking"""
    minio_config = get_minio_config()
    sf_config = get_snowflake_config()
    
    # Get stats from previous task
    ti = kwargs['ti']
    records_uploaded = ti.xcom_pull(task_ids='fetch_daily_data', key='records_uploaded') or 0
    fetch_errors = ti.xcom_pull(task_ids='fetch_daily_data', key='fetch_errors') or {}
    
    if records_uploaded == 0:
        logger.warning("No records were uploaded in previous task. Skipping load.")
        return 0
    
    logger.info(f"MinIO config - endpoint: {minio_config['endpoint_url']}, bucket: {minio_config['bucket']}")
    
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_config['endpoint_url'],
        aws_access_key_id=minio_config['aws_access_key_id'],
        aws_secret_access_key=minio_config['aws_secret_access_key']
    )
    
    try:
        response = s3.list_objects_v2(Bucket=minio_config['bucket'], Prefix='daily/')
        logger.info(f"List response keys: {response.keys()}")
    except Exception as e:
        logger.error(f"Error listing bucket: {e}", exc_info=True)
        error_msg = f"""
        **FAILED TO LIST MINIO OBJECTS**
        
        Error: {str(e)}
        
        **Action**: Check MinIO connectivity
        """
        send_slack_alert(error_msg, severity='critical', context=kwargs)
        raise
    
    if 'Contents' not in response:
        logger.warning(f"No 'Contents' key in response. Response: {response}")
        return 0
    
    logger.info(f"Total objects found: {len(response['Contents'])}")
    
    files_to_load = {}
    for obj in response['Contents']:
        if obj['Key'].endswith('.json'):
            parts = obj['Key'].split('/')
            logger.info(f"Processing {obj['Key']} - parts: {parts}")
            if len(parts) == 3:
                symbol = parts[1]
                date_str = parts[2].replace('.json', '')
                if symbol not in files_to_load or date_str > files_to_load[symbol][0]:
                    files_to_load[symbol] = (date_str, obj['Key'])
                    logger.info(f"Added to load queue: {symbol} - {date_str}")
    
    logger.info(f"Found {len(files_to_load)} symbols with data to load")
    
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
    records_loaded = 0
    records_updated = 0
    failed_loads = []
    load_errors = {}
    
    for symbol, (date_str, key) in files_to_load.items():
        data = None
        
        try:
            response = s3.get_object(Bucket=minio_config['bucket'], Key=key)
            data = json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            error_detail = str(e)[:100]
            load_errors[symbol] = error_detail
            logger.warning(f"Error reading {key}: {e}")
            failed_loads.append(symbol)
            continue
        
        if data is None:
            continue
        
        insert_success = False
        
        try:
            insert_sql = f"""
            INSERT INTO {sf_config['database']}.{sf_config['schema']}.DAILY_HISTORICAL_QUOTES
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
            logger.info(f"✓ Inserted {symbol} on {data['date']}")
            insert_success = True
            records_loaded += 1
        except Exception as e:
            logger.debug(f"Insert failed for {symbol}: {e}")
        
        if not insert_success:
            try:
                update_sql = f"""
                UPDATE {sf_config['database']}.{sf_config['schema']}.DAILY_HISTORICAL_QUOTES
                SET OPEN_PRICE = %s, HIGH_PRICE = %s, LOW_PRICE = %s, CLOSE_PRICE = %s, 
                    VOLUME = %s, FETCHED_AT = %s
                WHERE SYMBOL = %s AND DATE = %s
                """
                
                cur.execute(update_sql, (
                    data['o'],
                    data['h'],
                    data['l'],
                    data['c'],
                    data['v'],
                    datetime.fromtimestamp(data['fetched_at']),
                    data['symbol'],
                    data['date']
                ))
                logger.info(f"✓ Updated {symbol} on {data['date']}")
                records_updated += 1
            except Exception as e:
                error_detail = str(e)[:100]
                load_errors[symbol] = error_detail
                logger.error(f"Update failed for {symbol}: {e}")
                failed_loads.append(symbol)
                continue
    
    conn.commit()
    cur.close()
    conn.close()
    
    logger.info(f"✓ Loaded {records_loaded} records to Snowflake")
    logger.info(f"✓ Updated {records_updated} records in Snowflake")
    
    # Store results for success callback
    kwargs['ti'].xcom_push(key='records_loaded', value=records_loaded)
    kwargs['ti'].xcom_push(key='records_updated', value=records_updated)
    
    # Send completion alert
    total_processed = records_loaded + records_updated
    
    if total_processed == 0:
        severity = 'critical'
        error_details = "\n".join([f"  • {s}: {load_errors.get(s, 'Unknown')}" for s in failed_loads])
        message = f"""
        **DAILY LOAD FAILED - NO RECORDS LOADED**
        
        Files found: {len(files_to_load)}
        Failed loads: {len(failed_loads)}
        
        Failed symbols:
        {error_details}
        
        **Action**: Check Snowflake connectivity and data format
        """
    elif failed_loads:
        severity = 'warning'
        error_details = "\n".join([f"  • {s}: {load_errors.get(s, 'Unknown')}" for s in failed_loads])
        message = f"""
        **DAILY LOAD COMPLETED WITH ERRORS**
        
        New records inserted: {records_loaded}
        Records updated: {records_updated}
        Failed: {len(failed_loads)}
        
        Failed symbols:
        {error_details}
        """
    else:
        severity = 'info'
        message = f"""
        **DAILY PIPELINE SUCCESS** ✅
        
        New records inserted: {records_loaded}
        Records updated: {records_updated}
        Total processed: {total_processed}
        
        Next run: Tomorrow at 5:00 AM (market days only)
        """
    
    send_slack_alert(
        message=message,
        severity=severity,
        context=kwargs,
        additional_context={
            'records_loaded': records_loaded,
            'records_updated': records_updated,
            'data_source': 'yFinance'
        }
    )
    
    return total_processed

# ============================================
# DAG DEFINITION
# ============================================

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 15),
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
}

with DAG(
    'daily_historical_load',
    default_args=default_args,
    description='Daily load of D-1 historical data with enhanced Slack alerts',
    schedule_interval='0 5 * * 1-5',  # 5 AM on weekdays
    catchup=False,
    tags=['stocks', 'daily', 'historical', 'slack-enabled'],
) as dag:
    
    # ========== HEALTH CHECK TASKS ==========
    check_minio_task = PythonOperator(
        task_id='check_minio_connectivity',
        python_callable=check_minio_connectivity,
        provide_context=True,
        on_failure_callback=task_failure_callback,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    check_snowflake_task = PythonOperator(
        task_id='check_snowflake_connectivity',
        python_callable=check_snowflake_connectivity,
        provide_context=True,
        on_failure_callback=task_failure_callback,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    check_yfinance_task = PythonOperator(
        task_id='check_yfinance_api',
        python_callable=check_yfinance_api,
        provide_context=True,
        on_failure_callback=task_failure_callback,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    # ========== SETUP TASK ==========
    setup_task = PythonOperator(
        task_id='setup_snowflake_table',
        python_callable=setup_snowflake_table,
        provide_context=True,
        on_failure_callback=task_failure_callback,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        execution_timeout=timedelta(minutes=10),
    )
    
    # ========== MAIN ETL TASKS ==========
    fetch_task = PythonOperator(
        task_id='fetch_daily_data',
        python_callable=fetch_and_upload_daily_data,
        provide_context=True,
        on_failure_callback=task_failure_callback,
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=30),
    )
    
    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_daily_to_snowflake,
        provide_context=True,
        on_failure_callback=task_failure_callback,
        on_success_callback=task_success_callback,
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=30),
    )
    
    # ========== TASK DEPENDENCIES ==========
    # Health checks run in parallel, then setup, then ETL tasks
    [check_minio_task, check_snowflake_task, check_yfinance_task] >> setup_task >> fetch_task >> load_task