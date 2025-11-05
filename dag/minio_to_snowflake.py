import os
import json
import hashlib
import boto3
import snowflake.connector
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from typing import List, Dict, Set
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configure logging
logger = logging.getLogger(__name__)

# Constants
LOCAL_DIR = "/tmp/minio_downloads"
PROCESSED_FILES_TABLE = "processed_files_audit"
MINIO_PREFIX = "real_time/"

# Slack webhook URL
SLACK_WEBHOOK_URL = Variable.get("slack_webhook_url", default_var=None)

# ============================================
# SLACK NOTIFICATION FUNCTIONS
# ============================================

def send_slack_alert(message: str, severity: str = 'info', context: dict = None):
    """Send alert to Slack with color-coded formatting"""
    if not SLACK_WEBHOOK_URL:
        logger.warning("Slack webhook URL not configured. Skipping alert.")
        return
    
    color_map = {
        'info': '#36a64f',
        'warning': '#ff9900',
        'critical': '#ff0000'
    }
    
    emoji_map = {
        'info': '✅',
        'warning': '⚠️',
        'critical': '❌'
    }
    
    fields = []
    if context:
        if 'task_id' in context:
            fields.append({"title": "Task", "value": context['task_id'], "short": True})
        if 'dag_id' in context:
            fields.append({"title": "DAG", "value": context['dag_id'], "short": True})
        if 'execution_date' in context:
            fields.append({"title": "Execution Time", "value": str(context['execution_date']), "short": True})
    
    payload = {
        "attachments": [{
            "color": color_map.get(severity, '#36a64f'),
            "title": f"{emoji_map.get(severity, '📢')} Pipeline Alert - {severity.upper()}",
            "text": message,
            "fields": fields,
            "footer": "MinIO → Snowflake Pipeline",
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
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    
    message = f"""
    **TASK FAILED**
    
    Task: `{task_id}`
    DAG: `{dag_id}`
    Error: {str(exception)[:500]}
    
    **Action Required**: Check Airflow logs for details
    """
    
    send_slack_alert(message, severity='critical', context={
        'task_id': task_id,
        'dag_id': dag_id,
        'execution_date': execution_date
    })

def task_success_callback(context):
    """Called when pipeline completes successfully"""
    task_id = context.get('task_instance').task_id
    if task_id == 'dbt_run':
        message = "✅ **Complete pipeline execution successful** (Data load + dbt transformations)"
        send_slack_alert(message, severity='info')

def dbt_success_callback(context):
    """Called when dbt run succeeds"""
    message = """
    ✅ **dbt Transformations Completed**
    
    All dbt models have been successfully refreshed.
    Analytics tables are now up-to-date.
    """
    send_slack_alert(message, severity='info', context=context)

# ============================================
# MINIO & SNOWFLAKE CONFIG
# ============================================

def get_minio_config() -> Dict:
    """Get MinIO configuration from Airflow Variables or Connections"""
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
    """Get Snowflake configuration from Airflow Connection"""
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
        **MINIO UNREACHABLE**
        
        Error: {str(e)}
        Endpoint: {config.get('endpoint_url', 'unknown')}
        
        **Impact**: Cannot read data from MinIO
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
        **SNOWFLAKE UNREACHABLE**
        
        Error: {str(e)}
        Account: {config.get('account', 'unknown')}
        
        **Impact**: Cannot load data to Snowflake
        **Action**: Check Snowflake credentials and network
        """
        send_slack_alert(error_msg, severity='critical', context=kwargs)
        raise

def check_consumer_health(**kwargs):
    """Check if consumer.py is still running by reading heartbeat"""
    try:
        config = get_minio_config()
        s3 = boto3.client(
            "s3",
            endpoint_url=config['endpoint_url'],
            aws_access_key_id=config['aws_access_key_id'],
            aws_secret_access_key=config['aws_secret_access_key']
        )
        
        try:
            response = s3.get_object(
                Bucket=config['bucket'],
                Key='_heartbeat/consumer_status.json'
            )
            heartbeat_data = json.loads(response['Body'].read())
            
            last_heartbeat = datetime.fromisoformat(heartbeat_data['timestamp'])
            age_seconds = (datetime.utcnow() - last_heartbeat).total_seconds()
            
            MAX_AGE = 300  # 5 minutes
            
            if age_seconds > MAX_AGE:
                error_msg = f"""
                **CONSUMER CRASHED OR SLOW**
                
                Last heartbeat: {age_seconds:.0f} seconds ago (max: {MAX_AGE}s)
                Consumer ID: {heartbeat_data.get('consumer_id', 'unknown')}
                Messages processed: {heartbeat_data.get('messages_processed', 'unknown')}
                
                **Impact**: Real-time data is not being written to MinIO
                **Action**: SSH to consumer server and restart consumer.py
                """
                send_slack_alert(error_msg, severity='critical', context=kwargs)
                raise Exception(f"Consumer heartbeat too old: {age_seconds}s")
            
            logger.info(f"✓ Consumer healthy. Last heartbeat: {age_seconds:.0f}s ago")
            
        except s3.exceptions.NoSuchKey:
            logger.warning("⚠️ Consumer heartbeat file not found")
            warning_msg = """
            **CONSUMER HEARTBEAT MISSING**
            
            The consumer heartbeat file was not found in MinIO.
            
            **Possible causes**:
            - Consumer.py has never been started
            - Consumer.py is not writing heartbeat files
            
            **Action**: Verify consumer.py is running
            """
            send_slack_alert(warning_msg, severity='warning', context=kwargs)
            
    except Exception as e:
        if "Consumer heartbeat too old" in str(e):
            raise
        error_msg = f"""
        **CONSUMER HEALTH CHECK FAILED**
        
        Error: {str(e)}
        
        **Action**: Check MinIO connection and consumer.py status
        """
        send_slack_alert(error_msg, severity='critical', context=kwargs)
        raise

# ============================================
# SNOWFLAKE TABLE UTILITIES
# ============================================

def ensure_bronze_table_has_s3_key_column(sf_conn):
    """Add s3_source_key column if it doesn't exist"""
    cur = sf_conn.cursor()
    try:
        # Check if column exists
        cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'COMMON' 
        AND table_name = 'BRONZE_STOCK_QUOTES_RAW' 
        AND column_name = 'S3_SOURCE_KEY'
        """)
        
        if not cur.fetchone():
            logger.info("Adding s3_source_key column to bronze_stock_quotes_raw...")
            cur.execute("""
            ALTER TABLE bronze_stock_quotes_raw 
            ADD COLUMN s3_source_key VARCHAR(1000)
            """)
            logger.info("✓ Column s3_source_key added")
        else:
            logger.info("✓ Column s3_source_key already exists")
            
    except Exception as e:
        logger.error(f"Failed to add s3_source_key column: {e}")
        raise
    finally:
        cur.close()

def get_processed_records(sf_conn) -> Set[tuple]:
    """
    Get processed symbol+timestamp combinations (not file paths)
    Returns set of tuples: (symbol, timestamp)
    """
    cur = sf_conn.cursor()
    try:
        query = """
        SELECT DISTINCT 
            UPPER(TRIM(raw_json:symbol::VARCHAR)) as symbol,
            raw_json:fetched_at ::BIGINT as t_timestamp
        FROM bronze_stock_quotes_raw
        WHERE raw_json:symbol IS NOT NULL 
        AND raw_json:fetched_at  IS NOT NULL
        """
        cur.execute(query)
        
        processed_records = set()
        
        for row in cur.fetchall():
            symbol = row[0]  # Uppercase and trimmed
            t_timestamp = row[1]
            
            # Store as tuple: (SYMBOL, TIMESTAMP)
            processed_records.add((symbol, t_timestamp))
        
        logger.info(f"✓ Retrieved {len(processed_records)} unique symbol+timestamp combinations")
        
        # 🔍 DEBUG
        if processed_records:
            samples = list(processed_records)[:5]
            logger.info("Sample processed records:")
            for record in samples:
                logger.info(f"  ✓ ({record[0]}, {record[1]})")
        
        return processed_records
        
    except Exception as e:
        logger.error(f"Query failed: {e}")
        logger.exception(e)
        return set()
    finally:
        cur.close()

# ============================================
# FILE PROCESSING UTILITIES
# ============================================

def calculate_file_hash(filepath: str) -> str:
    """Calculate MD5 hash of a file for duplicate detection"""
    # ... (keep your existing function)
    pass

def get_file_metadata(filepath: str, s3_key: str) -> Dict:
    """Extract metadata from file"""
    # ... (keep your existing function)
    pass

def cleanup_local_files(file_paths: List[str]):
    """Clean up downloaded files from local storage"""
    deleted_count = 0
    failed_count = 0
    
    for filepath in file_paths:
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                deleted_count += 1
        except Exception as e:
            logger.warning(f"Failed to delete {filepath}: {e}")
            failed_count += 1
    
    logger.info(f"Cleanup completed: {deleted_count} files deleted, {failed_count} failed")

# ============================================
# MAIN ETL TASKS
# ============================================

def download_and_load_to_snowflake(**kwargs):
    os.makedirs(LOCAL_DIR, exist_ok=True)
    
    minio_config = get_minio_config()
    sf_config = get_snowflake_config()
    
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_config['endpoint_url'],
        aws_access_key_id=minio_config['aws_access_key_id'],
        aws_secret_access_key=minio_config['aws_secret_access_key']
    )
    
    # Get processed symbol+timestamp combinations from Snowflake
    logger.info("Step 1: Querying Snowflake for processed records...")
    try:
        sf_conn = snowflake.connector.connect(
            user=sf_config['user'],
            password=sf_config['password'],
            account=sf_config['account'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            schema=sf_config['schema'],
            role=sf_config['role']
        )
        
        processed_records = get_processed_records(sf_conn)  # Returns set of (symbol, timestamp) tuples
        
    except Exception as e:
        logger.error(f"Failed to query Snowflake: {e}")
        raise
    
    # List MinIO files
    logger.info("Step 2: Listing files in MinIO...")
    try:
        objects = []
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=minio_config['bucket'],
            Prefix=MINIO_PREFIX
        )
        
        for page in page_iterator:
            if 'Contents' in page:
                objects.extend(page['Contents'])
        
        logger.info(f"Found {len(objects)} total files in MinIO '{MINIO_PREFIX}'")
    except Exception as e:
        logger.error(f"Failed to list objects from MinIO: {e}")
        raise
    
    if not objects:
        logger.info("No files in MinIO")
        sf_conn.close()
        return
    
    # Filter files by parsing symbol and timestamp from MinIO path
    logger.info("Step 3: Filtering files by symbol+timestamp...")
    new_files = []
    skipped_count = 0
    parse_errors = 0
    
    for obj in objects:
        key = obj['Key']
        
        # Skip directories
        if key.endswith('/'):
            continue
        
        try:
            # Parse MinIO key format: real_time/{SYMBOL}/{TIMESTAMP}.json
            # Example: real_time/AMD/1760967046.json
            parts = key.split('/')
            
            if len(parts) >= 3:
                symbol = parts[1].upper()  # AMD, NVDA, etc.
                timestamp_str = parts[2].replace('.json', '')  # Remove .json extension
                timestamp = int(timestamp_str)
                
                # Check if this symbol+timestamp combination already exists
                record_key = (symbol, timestamp)
                
                if record_key in processed_records:
                    skipped_count += 1
                    logger.debug(f"Skipping: {symbol} @ {timestamp}")
                else:
                    new_files.append(obj)
            else:
                logger.warning(f"Cannot parse key: {key}")
                parse_errors += 1
                
        except Exception as e:
            logger.warning(f"Error parsing key '{key}': {e}")
            parse_errors += 1
            continue
    
    logger.info(f"""
    ========================================
    File Summary:
    Total files: {len(objects)}
    Parse errors: {parse_errors}
    Already processed: {skipped_count}
    New to download: {len(new_files)}
    ========================================
    """)
    
    if not new_files:
        logger.info("✓ All files already processed")
        sf_conn.close()
        return
    
    # Download files
    logger.info(f"Step 4: Downloading {len(new_files)} files...")
    file_metadata_list = []
    
    for idx, obj in enumerate(new_files, 1):
        key = obj["Key"]
        safe_filename = key.replace('/', '_')
        local_file = os.path.join(LOCAL_DIR, safe_filename)
        
        try:
            s3.download_file(minio_config['bucket'], key, local_file)
            file_metadata_list.append({
                'file_name': os.path.basename(local_file),
                's3_key': key,
                'local_path': local_file
            })
            
            if idx % 100 == 0:
                logger.info(f"Downloaded {idx}/{len(new_files)} files")
        except Exception as e:
            logger.error(f"Failed to download {key}: {e}")
    
    logger.info(f"✓ Downloaded {len(file_metadata_list)} files")
    
    # Upload to Snowflake in parallel
    logger.info(f"Step 5: Uploading to Snowflake with {20} parallel workers...")
    
    loaded_files = []
    failed_files = []
    lock = threading.Lock()
    
    def upload_single_file(metadata):
        try:
            cur = sf_conn.cursor()
            try:
                cur.execute(f"PUT file://{metadata['local_path']} @%bronze_stock_quotes_raw AUTO_COMPRESS=TRUE")
                with lock:
                    loaded_files.append(metadata)
                return True
            finally:
                cur.close()
        except Exception as e:
            logger.error(f"Failed to upload {metadata['file_name']}: {e}")
            with lock:
                failed_files.append(metadata['file_name'])
            return False
    
    max_workers = 20
    total_uploaded = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_metadata = {
            executor.submit(upload_single_file, metadata): metadata 
            for metadata in file_metadata_list
        }
        
        for future in as_completed(future_to_metadata):
            total_uploaded += 1
            if total_uploaded % 500 == 0:
                progress_pct = (total_uploaded / len(file_metadata_list)) * 100
                logger.info(f"Upload progress: {total_uploaded}/{len(file_metadata_list)} ({progress_pct:.1f}%)")
    
    logger.info(f"✓ Upload complete: {len(loaded_files)} succeeded, {len(failed_files)} failed")
    
    if not loaded_files:
        logger.error("No files uploaded")
        raise Exception("All uploads failed")
    
    # COPY INTO table
    logger.info("Step 6: Loading data with COPY INTO...")
    cur = sf_conn.cursor()
    try:
        cur.execute("""
            COPY INTO bronze_stock_quotes_raw (
                close_price,
                price_change,
                price_change_percent,
                high_price,
                low_price,
                open_price,
                prev_close,
                symbol,
                quote_timestamp,
                fetched_at,
                raw_json
            )
            FROM (
                SELECT 
                    $1:c::DECIMAL(10,2),
                    $1:d::DECIMAL(10,2),
                    $1:dp::DECIMAL(10,4),
                    $1:h::DECIMAL(10,2),
                    $1:l::DECIMAL(10,2),
                    $1:o::DECIMAL(10,2),
                    $1:pc::DECIMAL(10,2),
                    TRIM($1:symbol::VARCHAR),  
                    TO_TIMESTAMP_NTZ($1:t::INTEGER),
                    TO_TIMESTAMP_NTZ($1:fetched_at::INTEGER),
                    $1
                FROM @%bronze_stock_quotes_raw
            )
            FILE_FORMAT = (TYPE=JSON)
            ON_ERROR = CONTINUE
            FORCE = FALSE
            PURGE = TRUE
        """)
        
        results = cur.fetchall()
        logger.info(f"COPY INTO completed. Results: {results}")
        
        sf_conn.commit()
        logger.info(f"✓ Successfully loaded {len(loaded_files)} files")
        
        success_msg = f"""
        **DATA LOAD SUCCESS** ✅
        
        New files loaded: {len(loaded_files)}
        Failed: {len(failed_files)}
        
        Starting dbt transformations...
        """
        send_slack_alert(success_msg, severity='info', context=kwargs)
        
    except Exception as e:
        logger.error(f"COPY INTO failed: {e}")
        sf_conn.rollback()
        raise
    finally:
        cur.close()
        sf_conn.close()
        
        # Cleanup
        logger.info("Cleaning up local files...")
        cleanup_local_files([m['local_path'] for m in file_metadata_list])

# ============================================
# DAG DEFINITION
# ============================================

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 15),
    "email": ["data-alerts@company.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": task_failure_callback,
}

with DAG(
    dag_id="minio_to_snowflake_production",
    default_args=default_args,
    description="🚀 OPTIMIZED: MinIO → Snowflake → dbt transformations",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "etl", "minio", "snowflake", "dbt", "optimized"],
) as dag:
    
    # Health checks
    check_minio_task = PythonOperator(
        task_id="check_minio_connectivity",
        python_callable=check_minio_connectivity,
        provide_context=True,
    )
    
    check_snowflake_task = PythonOperator(
        task_id="check_snowflake_connectivity",
        python_callable=check_snowflake_connectivity,
        provide_context=True,
    )
    
    check_consumer_task = PythonOperator(
        task_id="check_consumer_health",
        python_callable=check_consumer_health,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    # Combined task - download and load to Snowflake
    download_and_load_task = PythonOperator(
        task_id="download_and_load",
        python_callable=download_and_load_to_snowflake,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )
    
    # ============================================
    # DBT TASK - Run transformations after data load
    # ============================================
    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command="docker exec dbt dbt run --profiles-dir /dbt --project-dir /dbt",
        on_success_callback=dbt_success_callback,
        on_failure_callback=task_failure_callback,
        execution_timeout=timedelta(minutes=15),
    )
    
    # Optional: dbt test task
    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command="docker exec dbt dbt test --profiles-dir /dbt --project-dir /dbt",
        on_failure_callback=task_failure_callback,
        execution_timeout=timedelta(minutes=10),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    # ============================================
    # DEPENDENCIES
    # ============================================
    # Health checks run in parallel
    [check_minio_task, check_snowflake_task] >> download_and_load_task
    
    # Consumer health check runs independently (doesn't block pipeline)
    check_consumer_task
    
    # After successful data load, run dbt transformations
    download_and_load_task >> dbt_run_task >> dbt_test_task