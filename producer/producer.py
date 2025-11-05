
import time
import json
import requests
import logging
import os
from datetime import datetime, time as dt_time
from kafka import KafkaProducer
from typing import Dict, List
import pytz
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
# ============================================
# TIMEZONE CONFIGURATION
# ============================================
ET = pytz.timezone('US/Eastern')
# ============================================
# CONFIGURATION FROM ENVIRONMENT
# ============================================
API_KEY = os.getenv("FINNHUB_API_KEY")
if not API_KEY:
    logger.error("FINNHUB_API_KEY environment variable not set!")
    exit(1)
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AMZN", "PANW", "NVDA", "AMD", "TSM"]
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
logger.info(f"Configuration:")
logger.info(f"  Kafka Broker: {KAFKA_BROKER}")
logger.info(f"  Symbols: {SYMBOLS}")
# ============================================
# MARKET HOURS (EASTERN TIME)
# ============================================
MARKET_OPEN = dt_time(9, 30)
MARKET_CLOSE = dt_time(16, 0)
MARKET_DAYS = [0, 1, 2, 3, 4]  # Mon-Fri
def is_market_open() -> bool:
    """Check if US stock market is currently open (using Eastern Time)."""
    now_et = datetime.now(ET)
    
    # Check if weekend
    if now_et.weekday() not in MARKET_DAYS:
        return False
    
    # Check if within market hours
    current_time = now_et.time()
    return MARKET_OPEN <= current_time <= MARKET_CLOSE
# ============================================
# POLLING STRATEGY
# ============================================
class PollingStrategy:
    MARKET_OPEN: int = 5       # Every 5 seconds during market
    MARKET_CLOSED: int = 300   # Every 5 minutes after hours
    WEEKEND: int = 3600        # Every 1 hour on weekends
    BEFORE_MARKET: int = 60    # Every 1 minute before market opens
    
    @staticmethod
    def get_interval() -> int:
        """Get appropriate polling interval based on Eastern Time."""
        now_et = datetime.now(ET)
        current_time = now_et.time()
        day_of_week = now_et.weekday()
        
        # Log current time for debugging
        logger.info(f"Current ET time: {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"Day of week: {now_et.strftime('%A')} (weekday={day_of_week})")
        
        # Check if weekday (Mon-Fri)
        if day_of_week in MARKET_DAYS:
            # Before market opens
            if current_time < dt_time(9, 30):
                logger.info("Status: Before market opens (9:30 AM ET)")
                return PollingStrategy.BEFORE_MARKET
            # During market hours
            elif dt_time(9, 30) <= current_time <= dt_time(16, 0):
                logger.info("Status: Market is open (9:30 AM - 4:00 PM ET)")
                return PollingStrategy.MARKET_OPEN
            # After market closes
            else:
                logger.info("Status: Market is closed (after 4:00 PM ET)")
                return PollingStrategy.MARKET_CLOSED
        
        # Weekend
        logger.info("Status: Weekend")
        return PollingStrategy.WEEKEND
# ============================================
# KAFKA PRODUCER WITH STARTUP WAIT
# ============================================
class ResilientKafkaProducer:
    def __init__(self, bootstrap_servers: List[str], max_retries: int = 10):
        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.producer = None
        self._connect()
    
    def _connect(self):
        # Wait for Kafka to start up
        logger.info("Waiting for Kafka to be ready...")
        time.sleep(10)
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Connecting to Kafka (attempt {attempt + 1}/{self.max_retries})...")
                
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                    request_timeout_ms=30000,
                    connections_max_idle_ms=540000
                )
                logger.info(f"✓ Connected to Kafka on {self.bootstrap_servers}")
                return
            
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1}/{self.max_retries} failed: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = 5 + (attempt * 2)
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("Failed to connect to Kafka after all retries")
                    raise
    
    def send_with_retry(self, topic: str, value: Dict) -> bool:
        if not self.producer:
            logger.error("Producer not connected")
            return False
        
        try:
            future = self.producer.send(topic, value=value)
            future.get(timeout=10)
            logger.debug(f"✓ Sent {value.get('symbol')} to Kafka")
            return True
        
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    def close(self):
        if self.producer:
            self.producer.close()
            logger.info("Producer closed")
# ============================================
# API FUNCTIONS
# ============================================
def fetch_quote(symbol):
    """Fetch stock quote from Finnhub API."""
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        logger.info(f"Fetching {symbol} from {url}")  # Add this line
        response = requests.get(url, timeout=10)
        logger.info(f"Response status: {response.status_code}")  # Add this
        response.raise_for_status()
        data = response.json()
        logger.info(f"Got data for {symbol}: {data}")  # Add this
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:
        logger.error(f"Error fetching {symbol}: {e}")
        return None
# ============================================
# MAIN
# ============================================
def main():
    logger.info("Starting Optimized Stock Quote Producer")
    
    producer = ResilientKafkaProducer(bootstrap_servers=[KAFKA_BROKER])
    
    try:
        while True:
            # Get polling interval based on market conditions
            poll_interval = PollingStrategy.get_interval()
            
            # Check if market is open
            if is_market_open():
                logger.info(f"Fetching quotes (interval: {poll_interval}s)")
                logger.info(f"SYMBOLS list: {SYMBOLS}")  # Add this
    
                for symbol in SYMBOLS:
                    logger.info(f"Processing symbol: {symbol}")  # Add this
                    quote = fetch_quote(symbol)
                    logger.info(f"Quote result for {symbol}: {quote}")  # Add this
                    if quote:
                        logger.info(f"Sending {symbol} to Kafka")  # Add this
                        producer.send_with_retry("stock-quotes", value=quote)
            
            else:
                logger.info(f"Market is not open. Next check in {poll_interval}s")
            
            time.sleep(poll_interval)
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        producer.close()
        logger.info("Producer stopped")
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        producer.close()
        raise
if __name__ == "__main__":
    main()
