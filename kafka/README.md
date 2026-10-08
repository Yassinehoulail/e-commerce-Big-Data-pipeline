# Kafka Streaming Pipeline

This directory contains the Kafka-based streaming implementation of the Bronze → Silver → Gold data pipeline.

## Architecture

The streaming pipeline uses Apache Kafka to enable real-time, event-driven data processing:

```
Scraper (Producer) → books-raw → Consumer (Bronze→Silver) → books-silver → Consumer (Silver→Gold) → books-gold → Consumer (Gold→DB) → MySQL
```

## Components

### 1. Kafka Topics

- **books-raw**: Raw scraped records from the scraper
- **books-silver**: Cleaned records after bronze→silver transformation
- **books-gold**: Fully enriched records ready for DB insertion

### 2. Consumers

- **consumer_bronze_to_silver.py**: Consumes from books-raw, applies cleaning logic, produces to books-silver
- **consumer_silver_to_gold.py**: Consumes from books-silver, applies enrichment logic, produces to books-gold
- **consumer_gold_to_db.py**: Consumes from books-gold, inserts into MySQL database

### 3. Modified Scraper

The scraper (`scraper/scraper.py`) has been modified to support both batch (CSV) and streaming (Kafka) modes.

## Setup

### 1. Install Dependencies

```bash
pip install confluent-kafka
```

Or install all dependencies from requirements.txt:

```bash
pip install -r requirements.txt
```

### 2. Start Docker Services

The docker-compose.yml has been extended to include Kafka services:

```bash
docker-compose up -d
```

This will start:
- Zookeeper (required by Kafka)
- Kafka broker (port 9092)
- Kafka UI (port 8081) - for topic monitoring
- MySQL (port 3306)
- phpMyAdmin (port 8080)

### 3. Verify Kafka Topics

The kafka-init container automatically creates the required topics. You can verify them using Kafka UI at http://localhost:8081 or via command line:

```bash
docker exec -it ecommerce_kafka kafka-topics --list --bootstrap-server localhost:9092
```

## Running the Streaming Pipeline

### Option 1: Run Consumers Individually

Open three separate terminal windows and run each consumer:

**Terminal 1 - Bronze to Silver:**
```bash
cd kafka
python consumer_bronze_to_silver.py
```

**Terminal 2 - Silver to Gold:**
```bash
cd kafka
python consumer_silver_to_gold.py
```

**Terminal 3 - Gold to Database:**
```bash
cd kafka
python consumer_gold_to_db.py
```

**Terminal 4 - Scraper (Producer):**
```bash
cd scraper
python scraper.py
```

### Option 2: Run with CSV Fallback (Batch Mode)

If you want to run the scraper without Kafka (batch mode only):

```bash
cd scraper
python scraper.py --no-kafka
```

This will only write to CSV files, maintaining backward compatibility with the original batch pipeline.

## Monitoring

### Kafka UI

Access the Kafka UI at http://localhost:8081 to monitor:
- Topics and their messages
- Consumer groups and their offsets
- Broker status

### Logs

Each consumer writes its own log file:
- `kafka/consumer_bronze_to_silver.log`
- `kafka/consumer_silver_to_gold.log`
- `kafka/consumer_gold_to_db.log`

### CSV Files

The streaming pipeline maintains CSV files as a fallback/traceability:
- `data/bronze/companies_revenue_bronze.csv` - Raw records
- `data/silver/companies_revenue_silver.csv` - Cleaned records
- `data/gold/companies_revenue_gold.csv` - Enriched records

## Configuration

### Kafka Configuration

All consumers use the following Kafka configuration (can be modified in each consumer file):

```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_GROUP_ID = "consumer-group-name"
KAFKA_AUTO_OFFSET_RESET = "earliest"
```

### Consumer Group IDs

- Bronze to Silver: `bronze-to-silver-consumer-group`
- Silver to Gold: `silver-to-gold-consumer-group`
- Gold to DB: `gold-to-db-consumer-group`

### Batch Sizes

Consumers process records in batches (default: 100 records) before flushing to CSV and committing offsets. This can be adjusted in each consumer file.

## Backward Compatibility

The original batch scripts remain fully functional:

- `transform/bronze_to_silver.py` - Batch transformation from CSV to CSV
- `transform/silver_to_gold.py` - Batch transformation from CSV to CSV
- `load_to_mysql.py` - Batch loading from CSV to MySQL

You can run either:
- **Streaming mode**: Kafka-based real-time processing
- **Batch mode**: CSV-based batch processing

Both modes write to the same CSV files, so you can switch between them as needed.

## Troubleshooting

### Kafka Connection Issues

If consumers fail to connect to Kafka:
1. Verify Kafka is running: `docker-compose ps`
2. Check Kafka logs: `docker-compose logs kafka`
3. Ensure port 9092 is not blocked by firewall

### MySQL Connection Issues

If the gold-to-db consumer fails to connect to MySQL:
1. Verify MySQL is running: `docker-compose ps mysql`
2. Check MySQL logs: `docker-compose logs mysql`
3. Verify connection credentials in `consumer_gold_to_db.py`

### Consumer Lag

If consumers are falling behind:
1. Check consumer lag in Kafka UI
2. Consider increasing batch sizes
3. Run multiple consumer instances (same group ID for load balancing)

## Stopping the Pipeline

To stop the streaming pipeline:

1. Stop the scraper (Ctrl+C in its terminal)
2. Stop each consumer (Ctrl+C in their terminals)
3. Optionally stop Docker services: `docker-compose down`

## Data Flow

1. **Scraper** scrapes books.toscrape.com and produces JSON messages to `books-raw` topic
2. **Bronze→Silver Consumer** consumes from `books-raw`, applies cleaning (dedup, null handling, standardization), produces to `books-silver`
3. **Silver→Gold Consumer** consumes from `books-silver`, applies enrichment (percentiles, z-scores, business metrics), produces to `books-gold`
4. **Gold→DB Consumer** consumes from `books-gold`, inserts/upserts records into MySQL `books_gold` table

Each step also appends to the corresponding CSV file for traceability and backup.
