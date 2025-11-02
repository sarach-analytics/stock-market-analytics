# End-to-End Stock Data Pipeline: From API to Power BI Dashboard

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![DBT](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?logo=powerbi&logoColor=black)

> A production-ready data engineering project demonstrating real-time streaming and batch processing with the Modern Data Stack.

---

## What This Project Does

This pipeline processes **real-time** and **historical** stock market data through a modern data architecture:

- **Real-time streaming**: Live stock quotes via Kafka → MinIO → Snowflake
- **Historical batch loads**: YTD and daily data directly to MinIO → Snowflake  
- **Data transformation**: DBT medallion architecture (Bronze → Silver → Gold)
- **Analytics**: Power BI dashboards with live and historical insights

**Special Thanks:** Inspired by [Jay's real-time stocks MDS project](https://github.com/Jay61616/real-time-stocks-mds/tree/main)

---

## Architecture

![Data Pipeline Architecture](assets/stock_data_architecture.png)

### Data Flow

**Real-Time Path:**  
`Finnhub API → Kafka → MinIO → Airflow → Snowflake → DBT → Power BI`

**Batch Path:**  
`yfinance API → MinIO → Airflow → Snowflake → DBT → Power BI`

Both flows converge in Snowflake's medallion architecture for unified analytics.

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Finnhub API key ([get free key](https://finnhub.io/))
- Snowflake account
- 8GB RAM minimum

### Setup (5 minutes)

1. **Clone and configure**
   ```bash
   git clone https://github.com/yourusername/real-time-stocks-pipeline.git
   cd real-time-stocks-pipeline
   cp .env.example .env
   # Edit .env with your FINNHUB_API_KEY and Snowflake credentials
   ```

2. **Launch all services**
   ```bash
   docker-compose up -d
   ```

3. **Initialize data pipeline**
   ```bash
   # Access Airflow at http://localhost:8080 (admin/admin)
   docker exec -it airflow-webserver airflow dags trigger setup_snowflake_schema
   docker exec -it airflow-webserver airflow dags trigger backfill_ytd_historical
   ```

4. **Run transformations**
   ```bash
   docker exec -it dbt_container dbt run
   docker exec -it dbt_container dbt test
   ```

**You're done!** Data flows automatically. Access:
- Airflow: `http://localhost:8080`
- MinIO Console: `http://localhost:9001`
- DBT Docs: `http://localhost:8080/docs`

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Ingestion** | Kafka, Python | Real-time streaming + batch API calls |
| **Storage** | MinIO, Snowflake | Object storage + cloud data warehouse |
| **Orchestration** | Airflow | Workflow automation |
| **Transformation** | DBT | SQL-based data modeling |
| **Visualization** | Power BI | Dashboards and analytics |
| **Infrastructure** | Docker | Containerized deployment |

---

## Data Models

### Bronze → Silver → Gold Layers

**Bronze (Raw)**
- `stg_stock_quotes_raw` - Real-time Kafka stream
- `stg_daily_historical_quotes_raw` - Daily batch loads
- `stg_ytd_historical_quotes_raw` - YTD backfill

**Silver (Cleaned)**
- `dim_stock_symbols` - Master stock dimension
- `stg_stock_quotes_cleaned` - Deduplicated real-time data
- `stg_historical_quotes_cleaned` - Standardized historical data

**Gold (Analytics-Ready)**
- `fact_daily_stock_quotes` - Daily OHLCV aggregations
- `fact_stock_performance` - Returns, volatility, KPIs
- `dim_date` / `dim_time` - Date and time dimensions

[View detailed data dictionary →](docs/DATA_MODELS.md)

---

## Project Structure

```text
├── producer/                   # Kafka producer (real-time)
├── consumer/                   # Kafka consumer (writes to MinIO)
├── dbt_stocks/                 # DBT transformations
│   ├── models/
│   │   ├── bronze/            # Raw staging
│   │   ├── silver/            # Cleaned data
│   │   └── gold/              # Analytics layer
├── dags/                       # Airflow DAGs
│   ├── backfill_ytd_historical.py
│   ├── daily_historical_load.py
│   └── minio_to_snowflake.py
├── docker-compose.yml
└── README.md
```

---

## Key Features

**Dual ingestion**: Real-time streaming + batch processing  
**Scalable architecture**: Handles millions of records  
**Data quality**: DBT tests and documentation  
**Automated workflows**: Airflow orchestration  
**Fully containerized**: One-command deployment  
**Production patterns**: Medallion architecture, idempotency, incremental loads

---

## Use Cases

- **Live monitoring**: Track stock prices in real-time
- **Historical analysis**: Identify trends and patterns
- **Backtesting**: Test trading strategies
- **Risk metrics**: Calculate volatility and returns
- **Comparative analysis**: Multi-stock performance comparison

---

## 🔧 Configuration

### Adjust Data Refresh Rates

Edit `dags/minio_to_snowflake.py`:
```python
schedule_interval='*/5 * * * *'  # Every 5 minutes (default)
```

Edit `dags/daily_historical_load.py`:
```python
schedule_interval='0 5 * * 1-5'  # 5 AM weekdays (default)
```

### Add More Stock Symbols

Edit `producer/producer.py`:
```python
SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'TSLA']  # Add your symbols
```

---

## Troubleshooting

**Services won't start:**
```bash
docker-compose down -v
docker-compose up -d
```

**Kafka messages not flowing:**
```bash
docker logs producer-container
docker logs kafka
```

**DBT tests failing:**
```bash
docker exec -it dbt_container dbt debug
docker exec -it dbt_container dbt run --debug
```

---

## Roadmap

- [ ] Add Streamlit dashboard
- [ ] Implement data quality monitoring
- [ ] Add CI/CD pipeline
- [ ] Support additional data sources (news sentiment)

---

## 📄 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

- **[Jay](https://github.com/Jay61616/real-time-stocks-mds)** - Original project inspiration
- **Finnhub.io** & **yfinance** - Stock market data APIs
- Modern Data Stack community for best practices

---

## 📧 Contact

- **Issues**: [GitHub Issues](https://github.com/yourusername/real-time-stocks-pipeline/issues)
- **LinkedIn**: [Your Profile](#https://www.linkedin.com/in/sarach-sriklab-b1669715a/)
- **Email**: sarach.srik@gmail.com

---

⭐ **If this project helped you, please give it a star!**

---

**Built with ❤️ using the Modern Data Stack**
