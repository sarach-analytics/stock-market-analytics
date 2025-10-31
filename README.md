# Stock Market Analytics & Data Pipeline

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![DBT](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?logo=powerbi&logoColor=black)
![MinIO](https://img.shields.io/badge/MinIO-C72E49?logo=minio&logoColor=white)

---

## 📊 Project Overview

This project demonstrates an **end-to-end data pipeline** built with the **Modern Data Stack (MDS)**, designed to handle both real-time and historical stock market data. The pipeline integrates two distinct data flows:

1. **Real-time streaming pipeline**: Captures live stock quotes from Finnhub API, streams through Apache Kafka, and processes in near real-time
2. **Historical data pipeline**: Fetches historical stock data directly from Finnhub API and loads it into MinIO, bypassing Kafka for efficient batch processing

Both data sources converge in Snowflake, where DBT transforms them through a **medallion architecture** (Bronze → Silver → Gold), delivering analytics-ready insights through Power BI dashboards.

**Special Thanks:** This project was inspired by and adapted from **[Jay's real-time stocks MDS project](https://github.com/Jay61616/real-time-stocks-mds/tree/main)**. His guidance and foundational work provided invaluable insights for building this pipeline.

---

## 🏗️ Architecture

![Data Pipeline Architecture](assets/stock_data_architecture.png)

### Dual Data Source Pipeline Flow

#### Real-Time Data Flow (Streaming)
1. **Data Ingestion**: Kafka producer continuously fetches live stock quotes from Finnhub API
2. **Event Streaming**: Apache Kafka streams messages between producer and consumer in real-time
3. **Data Landing**: Kafka consumer writes streaming data to MinIO (S3-compatible object storage)
4. **Orchestration**: Airflow DAG (`minio_to_snowflake.py`) moves real-time data from MinIO to Snowflake
5. **Transformation**: DBT processes data through Bronze → Silver → Gold layers
6. **Visualization**: Power BI displays live metrics and KPIs

#### Historical Data Flow (Batch)
1. **Batch Ingestion**: Python scripts fetch historical stock data directly from Finnhub API
2. **Direct Storage**: Historical data writes directly to MinIO, bypassing Kafka for efficiency
3. **Scheduled Load**: Airflow DAGs (`backfill_ytd_historical.py`, `daily_historical_load.py`) orchestrate batch loads to Snowflake
4. **Transformation**: DBT models process historical data alongside real-time data
5. **Analytics**: Combined datasets enable comprehensive trend analysis and backtesting

---

## 🛠️ Tech Stack

| Technology | Purpose |
|------------|---------|
| **Docker** | Containerized deployment for consistency and portability |
| **Apache Kafka** | Real-time message streaming for live stock quotes |
| **MinIO** | S3-compatible object storage for both streaming and batch data |
| **Snowflake** | Cloud data warehouse for centralized storage and analytics |
| **DBT** | SQL-based data transformation and modeling framework |
| **Apache Airflow** | Workflow orchestration for both real-time and batch pipelines |
| **Python** | API integration, data processing, and automation scripts |
| **Power BI** | Business intelligence dashboards and interactive visualizations |
| **PostgreSQL** | Airflow metadata database |
| **Finnhub API** | Data source for both real-time quotes and historical market data |

---

## ✨ Key Features

- ⚡ **Dual data ingestion**: Real-time streaming via Kafka + batch historical loads
- 🔄 **Hybrid architecture**: Optimized data flow for different data types (streaming vs. batch)
- 📊 **Comprehensive coverage**: Live quotes combined with year-to-date historical data
- 🏅 **Medallion architecture**: Bronze, Silver, and Gold layers for data quality
- 🔧 **Automated workflows**: Airflow orchestrates both real-time and batch pipelines
- 📐 **Data quality assurance**: DBT tests and documentation across all layers
- 📈 **Historical backfill**: Year-to-date data loading for trend analysis and backtesting
- 🎯 **Unified analytics**: Single source of truth in Snowflake combining both data sources
- 🐳 **Fully containerized**: Easy deployment and reproducibility with Docker
- 📊 **Live dashboards**: Power BI visualizations with real-time and historical insights

---

## 📁 Repository Structure

```text
real-time-stocks-pipeline/
├── producer/                              # Kafka producer for real-time data
│   └── producer.py                        # Streams live stock quotes via Kafka
├── consumer/                              # Kafka consumer for MinIO storage
│   └── consumer.py                        # Consumes Kafka messages, writes to MinIO
├── dbt_stocks/                            # DBT project for transformations
│   ├── models/
│   │   ├── bronze/                        # Raw data staging layer
│   │   │   ├── stg_daily_historical_quotes_raw.sql      # Daily historical data
│   │   │   ├── stg_stock_quotes_raw.sql                 # Real-time quotes
│   │   │   └── stg_ytd_historical_quotes_raw.sql        # YTD historical backfill
│   │   ├── silver/                        # Cleaned and standardized layer
│   │   │   ├── dim_stock_symbols.sql                    # Stock dimension table
│   │   │   ├── stg_historical_quotes_cleaned.sql        # Cleaned historical data
│   │   │   └── stg_stock_quotes_cleaned.sql             # Cleaned real-time data
│   │   └── gold/                          # Analytics-ready layer
│   │       ├── dim_date.sql                             # Date dimension
│   │       ├── dim_time.sql                             # Time dimension
│   │       ├── fact_daily_stock_quotes.sql              # Daily aggregated facts
│   │       └── fact_stock_performance.sql               # Performance metrics
│   ├── dbt_project.yml                    # DBT configuration
│   └── profiles.yml                       # Snowflake connection profile
├── dags/                                  # Airflow DAGs
│   ├── backfill_ytd_historical.py         # YTD historical data backfill (batch)
│   ├── daily_historical_load.py           # Daily historical batch processing
│   └── minio_to_snowflake.py              # Real-time data transfer from MinIO
├── scripts/                               # Utility scripts (optional)
│   └── historical_loader.py               # Direct historical data loader to MinIO
├── config/                                # Configuration files
│   ├── kafka_config.yml                   # Kafka settings
│   └── stock_symbols.json                 # List of stocks to track
├── docker-compose.yml                     # Multi-container orchestration
├── Dockerfile.consumer                    # Consumer container image
├── Dockerfile.producer                    # Producer container image
├── requirements.txt                       # Python dependencies
├── .env.example                           # Environment variables template
├── .gitignore                             # Git ignore rules
└── README.md                              # Project documentation
```

---

## 🚀 Getting Started

### Prerequisites

- Docker and Docker Compose installed
- Finnhub API key ([Get one here](https://finnhub.io/))
- Snowflake account with appropriate permissions
- Power BI Desktop (optional, for visualization)
- Minimum 8GB RAM and 20GB disk space

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/real-time-stocks-pipeline.git
   cd real-time-stocks-pipeline
   ```

2. **Set up environment variables**
   ```bash
   cp .env.example .env
   ```
   
   Edit `.env` with your credentials:
   ```env
   # Finnhub API
   FINNHUB_API_KEY=your_api_key_here
   
   # Snowflake
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=STOCK_DATA
   SNOWFLAKE_SCHEMA=RAW
   
   # MinIO
   MINIO_ACCESS_KEY=minioadmin
   MINIO_SECRET_KEY=minioadmin
   
   # Kafka
   KAFKA_BROKER=kafka:9092
   ```

3. **Launch the pipeline**
   ```bash
   docker-compose up -d
   ```

4. **Verify all services are running**
   ```bash
   docker-compose ps
   ```

5. **Access service UIs**
   - **Airflow**: `http://localhost:8080` (default: admin/admin)
   - **MinIO Console**: `http://localhost:9001`
   - **Kafka UI**: `http://localhost:8090` (if configured)

6. **Initialize Snowflake schema**
   ```bash
   docker exec -it airflow-webserver airflow dags trigger setup_snowflake_schema
   ```

7. **Start data ingestion**
   
   **Real-time pipeline:**
   - Producer and consumer start automatically with Docker Compose
   
   **Historical pipeline:**
   ```bash
   # One-time YTD backfill
   docker exec -it airflow-webserver airflow dags trigger backfill_ytd_historical
   
   # Enable daily historical loads
   docker exec -it airflow-webserver airflow dags unpause daily_historical_load
   ```

8. **Run DBT transformations**
   ```bash
   docker exec -it dbt_container dbt run
   docker exec -it dbt_container dbt test
   docker exec -it dbt_container dbt docs generate
   docker exec -it dbt_container dbt docs serve
   ```

---

## 📊 Data Models

### Bronze Layer (Raw Data)
- **`stg_stock_quotes_raw`**: Real-time stock quotes from Kafka stream
- **`stg_daily_historical_quotes_raw`**: Daily historical data loaded in batches
- **`stg_ytd_historical_quotes_raw`**: Year-to-date backfill data

### Silver Layer (Cleaned & Standardized)
- **`dim_stock_symbols`**: Master dimension table for all tracked stocks
- **`stg_stock_quotes_cleaned`**: Cleaned and deduplicated real-time data
- **`stg_historical_quotes_cleaned`**: Standardized historical quotes with type casting

### Gold Layer (Analytics-Ready)
- **`dim_date`**: Date dimension with calendar attributes
- **`dim_time`**: Time dimension for intraday analysis
- **`fact_daily_stock_quotes`**: Daily aggregated metrics (OHLCV)
- **`fact_stock_performance`**: Performance KPIs, returns, and volatility metrics

---

## 🔄 Data Pipeline Workflows

### Real-Time Workflow
```
Finnhub API → Kafka Producer → Kafka Topic → Kafka Consumer → MinIO → Airflow → Snowflake (Bronze) → DBT → Gold Layer → Power BI
```

**Frequency**: Continuous streaming (every few seconds)

**DAG**: `minio_to_snowflake.py` (runs every 5 minutes)

### Historical Workflow
```
Finnhub API → Python Script → MinIO → Airflow → Snowflake (Bronze) → DBT → Gold Layer → Power BI
```

**Frequency**: 
- Daily batch loads (end of trading day)
- One-time YTD backfill (on-demand)

**DAGs**: 
- `backfill_ytd_historical.py` (manual trigger)
- `daily_historical_load.py` (scheduled daily)

### DBT Transformation Workflow
```
Bronze (Raw) → Silver (Cleaned) → Gold (Analytics) → Power BI Dashboards
```

**Schedule**: Triggered after data loads complete (via Airflow sensors)

---

## 🎯 Use Cases

- **Real-time monitoring**: Track live stock prices and market movements
- **Historical analysis**: Analyze trends, patterns, and seasonality
- **Backtesting**: Test trading strategies against historical data
- **Performance metrics**: Calculate returns, volatility, and risk indicators
- **Comparative analysis**: Compare multiple stocks across time periods
- **Alerting**: Set up notifications for price movements (extensible)

---

## 📈 Sample Analytics & KPIs

Power BI dashboards include:
- Live stock price tickers
- Daily price change and percentage movement
- Volume analysis and trends
- Historical price charts (candlestick, line)
- Moving averages (SMA, EMA)
- Volatility indicators
- Year-to-date performance
- Sector and symbol comparisons

---

## 🧪 Testing

Run DBT tests to ensure data quality:

```bash
# Run all tests
docker exec -it dbt_container dbt test

# Run tests for specific model
docker exec -it dbt_container dbt test --select stg_stock_quotes_cleaned

# Run tests for specific layer
docker exec -it dbt_container dbt test --select silver.*
```

---

## 📝 Configuration

### Adding New Stock Symbols

Edit `config/stock_symbols.json`:
```json
{
  "symbols": ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "YOUR_SYMBOL"]
}
```

### Adjusting Data Refresh Intervals

Edit `dags/minio_to_snowflake.py`:
```python
schedule_interval='*/5 * * * *'  # Every 5 minutes (default)
```

Edit `dags/daily_historical_load.py`:
```python
schedule_interval='0 18 * * 1-5'  # 6 PM weekdays (default)
```

---

## 🛠️ Troubleshooting

### Common Issues

**Issue**: Kafka consumer not receiving messages
```bash
# Check producer logs
docker logs producer-container

# Verify Kafka topic
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

**Issue**: Airflow DAG failing
```bash
# Check Airflow logs
docker logs airflow-webserver
docker logs airflow-scheduler

# Test connection
docker exec -it airflow-webserver airflow connections test snowflake_default
```

**Issue**: DBT models failing
```bash
# Run with debug mode
docker exec -it dbt_container dbt run --debug

# Check Snowflake connection
docker exec -it dbt_container dbt debug
```

---

## 🚧 Future Enhancements

- [ ] Add machine learning models for price prediction
- [ ] Implement real-time alerting system (email/Slack)
- [ ] Add more data sources (news sentiment, social media)
- [ ] Build custom web dashboard (Streamlit/Dash)
- [ ] Implement data versioning and lineage tracking
- [ ] Add unit tests for Python scripts
- [ ] Set up CI/CD pipeline
- [ ] Add data quality monitoring (Great Expectations)

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please ensure your code follows the existing style and includes appropriate tests.

---

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **[Jay](https://github.com/Jay61616/real-time-stocks-mds)** for the original project inspiration and guidance
- **Finnhub.io** for providing comprehensive stock market API
- The open-source community for the amazing tools that power this pipeline
- Modern Data Stack community for best practices and patterns

---

## 📧 Contact

For questions, suggestions, or collaboration:

- Open an issue in this repository
- Reach out via [your email or social media]
- Connect on LinkedIn: [your profile]

---

## ⭐ Show Your Support

If you find this project helpful, please consider giving it a star! It helps others discover the project and motivates continued development.

---

**Built with ❤️ using the Modern Data Stack**
