# Stock Market Analytics & Data Pipeline

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![DBT](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?logo=powerbi&logoColor=black)

---

## Project Overview  
This project showcases an **end-to-end real-time data pipeline** built with the **Modern Data Stack**.  
It captures **live stock market data** from an external API, streams it in real time, orchestrates data transformations, and delivers analytics-ready insights — all within a unified framework.  

Special thanks to **[Jay](https://github.com/Jay61616/real-time-stocks-mds/tree/main)** for his guidance and the foundational work. Parts of the code and pipeline design were adapted from his original project.  

## Architecture
![Data Pipeline Architecture](assets/stock_data_architecture.png)

## Tech Stack
This project integrates a modern, cloud-based data pipeline using the following tools:

- **Docker** – Containerized deployment for consistency and portability
-  **Apache Kafka** – Real-time message streaming between producer and consumer  
- **Snowflake** – Centralized data warehouse for storage and analytics
- **DBT** – SQL-based data transformation and modeling  
- **Apache Airflow** – Orchestration and scheduling for automated workflows  
- **Python** – API data extraction, transformation scripts, and process automation  
- **Power BI** – Visualization layer for delivering business insights  

---

## Key Features
- Streams **real-time stock market data** directly from the Finnhub API  
- Establishes a **scalable data pipeline** built with Kafka and Airflow  
- Applies **structured data transformations** with DBT on Snowflake  
- Automates the ETL process through Airflow DAGs  
- Maintains modular architecture across Bronze, Silver, and Gold layers  
- Connects Power BI for **live reporting and KPI tracking**  

---

## Repository Structure
Below is the structure of the repository, showing all key components of the pipeline:

```text
real-time-stocks-pipeline/
├── producer/                     # Kafka producer for Finnhub API data
│   └── producer.py
├── consumer/                     # Kafka consumer writing to MinIO
│   └── consumer.py
├── dbt_stocks/models/
│   ├── bronze
│       ├── stg_daily_historical_quotes_raw.sql
│       ├── stg_stock_quotes_raw.sql
│       └── stg_ytd_historical_quotes_raw.sql
│   ├── silver
│       ├── dim_stock_symbols.sql
│       ├── stg_historical_quotes_cleaned.sql
│       └── stg_stock_quotes_cleaned.sql
│   └── gold
│       ├── dim_date.sql
│       ├── dim_time.sql
│       ├── fact_daily_stock_quotes.sql
│       └── fact_stock_performance.sql
├── dag/
│       ├── backfill_ytd_historical.py
│       ├── daily_historical_load.py
│       └── minio_to_snowflake.py
├── docker-compose.yml            # Configuration for Kafka, Airflow, MinIO, Postgres
├── Dockerfile.consumer
├── Dockerfile.producer
├── requirements.txt
└── README.md
