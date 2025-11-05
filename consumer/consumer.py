import json
import boto3
import time
from kafka import KafkaConsumer
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION FROM ENVIRONMENT
# ============================================
# Use environment variables (set by Docker Compose)
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
BUCKET_NAME = "bronze-transactions"

# Heartbeat configuration
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "100"))  # Write heartbeat every N messages
HEARTBEAT_KEY = "_heartbeat/consumer_status.json"

logger.info(f"Configuration:")
logger.info(f"  Kafka Broker: {KAFKA_BROKER}")
logger.info(f"  MinIO Endpoint: {MINIO_ENDPOINT}")
logger.info(f"  Heartbeat Interval: {HEARTBEAT_INTERVAL} messages")

# ============================================
# MINIO SETUP
# ============================================
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Ensure bucket exists
try:
    s3.head_bucket(Bucket=BUCKET_NAME)
    logger.info(f"✓ Bucket '{BUCKET_NAME}' exists")
except Exception as e:
    logger.info(f"Creating bucket '{BUCKET_NAME}'...")
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        logger.info(f"✓ Bucket '{BUCKET_NAME}' created")
    except Exception as e:
        logger.error(f"✗ Failed to create bucket: {e}")

# ============================================
# HEARTBEAT FUNCTION
# ============================================
def write_heartbeat(message_count, error_count=0):
    """
    Write heartbeat file to MinIO to indicate consumer is alive.
    Airflow will check this file to monitor consumer health.
    """
    try:
        heartbeat_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'consumer_id': 'bronze-consumer1',
            'status': 'healthy',
            'messages_processed': message_count,
            'error_count': error_count,
            'kafka_broker': KAFKA_BROKER,
            'last_update': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        }
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=HEARTBEAT_KEY,
            Body=json.dumps(heartbeat_data, indent=2),
            ContentType='application/json'
        )
        
        logger.debug(f"✓ Heartbeat written (messages: {message_count}, errors: {error_count})")
        
    except Exception as e:
        logger.warning(f"⚠️ Failed to write heartbeat: {e}")
        # Don't crash consumer if heartbeat fails - just log warning

# ============================================
# KAFKA CONSUMER
# ============================================
logger.info(f"Connecting to Kafka at {KAFKA_BROKER}...")
max_retries = 5
retry_count = 0

while retry_count < max_retries:
    try:
        consumer = KafkaConsumer(
            "stock-quotes",
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="bronze-consumer1",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            session_timeout_ms=30000,
            request_timeout_ms=60000,
            connections_max_idle_ms=540000
        )
        logger.info("✓ Connected to Kafka successfully!")
        break
    except Exception as e:
        retry_count += 1
        logger.warning(f"✗ Kafka connection failed (attempt {retry_count}/{max_retries}): {e}")
        if retry_count < max_retries:
            logger.info(f"  Retrying in 5 seconds...")
            time.sleep(5)
        else:
            logger.error(f"✗ Failed to connect to Kafka after {max_retries} attempts")
            logger.error(f"  Make sure Kafka is running on {KAFKA_BROKER}")
            exit(1)

# ============================================
# MAIN CONSUMER LOOP
# ============================================
logger.info("Listening for messages from 'stock-quotes' topic...")
logger.info(f"Heartbeat will be written every {HEARTBEAT_INTERVAL} messages")

record_count = 0
error_count = 0

# Write initial heartbeat to indicate consumer started
write_heartbeat(message_count=0, error_count=0)
logger.info(f"✓ Initial heartbeat written to s3://{BUCKET_NAME}/{HEARTBEAT_KEY}")

try:
    for message in consumer:
        try:
            record = message.value
            symbol = record.get("symbol", "unknown")
            ts = record.get("fetched_at", int(time.time()))
            key = f"real_time/{symbol}/{ts}.json"
            
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=key,
                Body=json.dumps(record),
                ContentType="application/json"
            )
            
            record_count += 1
            
            # Write heartbeat every N messages
            if record_count % HEARTBEAT_INTERVAL == 0:
                write_heartbeat(message_count=record_count, error_count=error_count)
                logger.info(f"✓ Saved {record_count} records to MinIO | Heartbeat updated")
            elif record_count % 100 == 0:
                logger.info(f"✓ Saved {record_count} records to MinIO")
            else:
                logger.debug(f"✓ Saved {symbol} → s3://{BUCKET_NAME}/{key}")
        
        except Exception as e:
            error_count += 1
            logger.error(f"✗ Error processing message: {e}")
            
            # Write heartbeat on error too (to update error_count)
            if error_count % 10 == 0:
                write_heartbeat(message_count=record_count, error_count=error_count)
                logger.warning(f"⚠️ Error count: {error_count} | Heartbeat updated")
            
            continue

except KeyboardInterrupt:
    logger.info("Shutting down consumer...")
    # Write final heartbeat before shutdown
    write_heartbeat(message_count=record_count, error_count=error_count)
    logger.info("✓ Final heartbeat written")

except Exception as e:
    logger.error(f"✗ Consumer error: {e}")
    # Try to write heartbeat even on crash
    try:
        write_heartbeat(message_count=record_count, error_count=error_count)
    except:
        pass

finally:
    consumer.close()
    logger.info(f"✓ Consumer closed. Total records saved: {record_count}, Errors: {error_count}")