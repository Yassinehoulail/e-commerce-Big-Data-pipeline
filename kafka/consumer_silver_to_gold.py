"""
Consumer Silver → Gold
Consomme les messages nettoyés depuis books-silver, applique l'enrichissement analytique,
et produit vers books-gold
"""

import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys
from collections import defaultdict

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Kafka support
try:
    from confluent_kafka import Consumer, Producer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("Error: confluent-kafka not installed. Please install it to run this consumer.")
    exit(1)

# MinIO support
try:
    from minio.minio_client import upload_file
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    print("Warning: MinIO client not available. CSV backup only mode.")

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('consumer_silver_to_gold.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_GROUP_ID = "silver-to-gold-consumer-group"
KAFKA_TOPIC_SILVER = "books-silver"
KAFKA_TOPIC_GOLD = "books-gold"

# Configuration CSV
SILVER_CSV_PATH = "../data/silver/companies_revenue_silver.csv"
GOLD_CSV_PATH = "../data/gold/companies_revenue_gold.csv"

# In-memory state for analytics calculations
silver_records_buffer = []
gold_records = []
batch_size = 100

# Statistics cache for percentile calculations
stats_cache = {
    'revenue_values': [],
    'total_revenue': 0,
    'count': 0
}

def create_consumer():
    """
    Create and return a Kafka consumer
    """
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    try:
        consumer = Consumer(conf)
        consumer.subscribe([KAFKA_TOPIC_SILVER])
        logger.info(f"Kafka consumer created and subscribed to {KAFKA_TOPIC_SILVER}")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        return None

def create_producer():
    """
    Create and return a Kafka producer
    """
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'silver-to-gold-producer',
        'acks': 'all'
    }
    
    try:
        producer = Producer(conf)
        logger.info("Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None

def delivery_report(err, msg):
    """
    Callback function for Kafka delivery reports
    """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def calculate_analytics(record, stats_cache):
    """
    Calculate analytics metrics for a record
    Uses sliding window statistics for streaming context
    """
    revenue = float(record.get('revenue', 0))
    
    # Update statistics cache
    stats_cache['revenue_values'].append(revenue)
    stats_cache['total_revenue'] += revenue
    stats_cache['count'] += 1
    
    # Keep only last 1000 values for sliding window
    if len(stats_cache['revenue_values']) > 1000:
        removed = stats_cache['revenue_values'].pop(0)
        stats_cache['total_revenue'] -= removed
        stats_cache['count'] -= 1
    
    # Calculate statistics from cache
    revenue_values = stats_cache['revenue_values']
    if len(revenue_values) > 0:
        mean_revenue = stats_cache['total_revenue'] / stats_cache['count']
        std_revenue = np.std(revenue_values) if len(revenue_values) > 1 else 1.0
        
        # Calculate percentile (simplified for streaming)
        sorted_values = sorted(revenue_values)
        percentile = (sorted_values.index(revenue) + 1) / len(sorted_values) * 100
        
        # Calculate Z-score
        zscore = (revenue - mean_revenue) / std_revenue if std_revenue > 0 else 0
        
        # Revenue category based on percentiles
        if percentile < 25:
            category = 'Low'
        elif percentile < 50:
            category = 'Medium-Low'
        elif percentile < 75:
            category = 'Medium-High'
        else:
            category = 'High'
        
        record['revenue_percentile'] = round(percentile, 2)
        record['revenue_category'] = category
        record['revenue_zscore'] = round(zscore, 4)
    else:
        record['revenue_percentile'] = 50.0
        record['revenue_category'] = 'Medium'
        record['revenue_zscore'] = 0.0
    
    # Revenue per sale
    sales = int(record.get('sales', 1))
    if sales > 0:
        record['revenue_per_sale'] = round(revenue / sales, 2)
    else:
        record['revenue_per_sale'] = 0.0
    
    # Estimated profit (20% margin)
    record['estimated_profit'] = round(revenue * 0.20, 2)
    
    return record

def add_business_metrics(record, stats_cache):
    """
    Add business metrics to the record
    """
    revenue = float(record.get('revenue', 0))
    total_revenue = stats_cache['total_revenue']
    
    # Revenue percentage of total
    if total_revenue > 0:
        record['revenue_percentage'] = round((revenue / total_revenue) * 100, 4)
    else:
        record['revenue_percentage'] = 0.0
    
    # Revenue rank (simplified for streaming)
    revenue_values = sorted(stats_cache['revenue_values'], reverse=True)
    if revenue in revenue_values:
        record['revenue_rank'] = revenue_values.index(revenue) + 1
    else:
        record['revenue_rank'] = stats_cache['count']
    
    # Top performer (top 10%)
    percentile = record.get('revenue_percentile', 50)
    record['is_top_performer'] = percentile >= 90
    
    # Revenue difference from average
    mean_revenue = total_revenue / stats_cache['count'] if stats_cache['count'] > 0 else 0
    record['revenue_diff_from_avg'] = round(revenue - mean_revenue, 2)
    
    return record

def apply_enrichment(record, stats_cache):
    """
    Apply all enrichment transformations to a record
    """
    try:
        # Calculate analytics
        record = calculate_analytics(record, stats_cache)
        
        # Add business metrics
        record = add_business_metrics(record, stats_cache)
        
        return record
    except Exception as e:
        logger.error(f"Error enriching record: {e}")
        return None

def produce_to_gold(producer, record):
    """
    Produce an enriched record to the gold topic
    """
    try:
        value = json.dumps(record, default=str).encode('utf-8')
        producer.produce(
            KAFKA_TOPIC_GOLD,
            value=value,
            callback=delivery_report
        )
        return True
    except Exception as e:
        logger.error(f"Failed to produce to gold topic: {e}")
        return False

def append_to_silver_csv(record):
    """
    Append silver record to CSV for traceability
    """
    try:
        os.makedirs(os.path.dirname(SILVER_CSV_PATH), exist_ok=True)
        
        file_exists = os.path.exists(SILVER_CSV_PATH)
        
        df = pd.DataFrame([record])
        df.to_csv(SILVER_CSV_PATH, mode='a', header=not file_exists, index=False, encoding='utf-8')
        
        # Sync to MinIO (non-blocking, wrapped in try/except)
        if MINIO_AVAILABLE:
            try:
                object_name = os.path.basename(SILVER_CSV_PATH)
                upload_file("silver", object_name, SILVER_CSV_PATH)
            except Exception as e:
                logger.warning(f"Failed to sync silver CSV to MinIO: {e}")
        
    except Exception as e:
        logger.error(f"Failed to append to silver CSV: {e}")

def append_to_gold_csv(records):
    """
    Append gold records to CSV
    """
    try:
        if not records:
            return
        
        os.makedirs(os.path.dirname(GOLD_CSV_PATH), exist_ok=True)
        
        file_exists = os.path.exists(GOLD_CSV_PATH)
        
        df = pd.DataFrame(records)
        df.to_csv(GOLD_CSV_PATH, mode='a', header=not file_exists, index=False, encoding='utf-8')
        
        # Sync to MinIO (non-blocking, wrapped in try/except)
        if MINIO_AVAILABLE:
            try:
                object_name = os.path.basename(GOLD_CSV_PATH)
                upload_file("gold", object_name, GOLD_CSV_PATH)
            except Exception as e:
                logger.warning(f"Failed to sync gold CSV to MinIO: {e}")
        
    except Exception as e:
        logger.error(f"Failed to append to gold CSV: {e}")

def process_message(producer, msg, stats_cache):
    """
    Process a single Kafka message
    """
    try:
        # Deserialize JSON
        record = json.loads(msg.value().decode('utf-8'))
        
        # Append to silver CSV for traceability
        append_to_silver_csv(record)
        
        # Apply enrichment transformations
        enriched_record = apply_enrichment(record, stats_cache)
        
        if enriched_record is None:
            logger.error("Failed to enrich record, skipping")
            return
        
        # Produce to gold topic
        if produce_to_gold(producer, enriched_record):
            gold_records.append(enriched_record)
            logger.info(f"Processed and sent to gold: {record.get('title')}")
        
        # Flush batch if size reached
        if len(gold_records) >= batch_size:
            producer.flush()
            append_to_gold_csv(gold_records)
            logger.info(f"Batch of {len(gold_records)} records appended to gold CSV")
            gold_records.clear()
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main():
    """
    Main consumer loop
    """
    logger.info("=" * 60)
    logger.info("CONSUMER SILVER → GOLD")
    logger.info("=" * 60)
    
    # Create consumer and producer
    consumer = create_consumer()
    producer = create_producer()
    
    if consumer is None or producer is None:
        logger.error("Failed to initialize consumer or producer")
        return
    
    # Initialize statistics cache
    stats_cache = {
        'revenue_values': [],
        'total_revenue': 0,
        'count': 0
    }
    
    try:
        logger.info("Starting consumer loop...")
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # No message, continue
                producer.poll(0)
                continue
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            # Process the message
            process_message(producer, msg, stats_cache)
            
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Flush remaining records
        if gold_records:
            producer.flush()
            append_to_gold_csv(gold_records)
            logger.info(f"Flushed final batch of {len(gold_records)} records")
        
        # Close consumer and producer
        consumer.close()
        producer.flush()
        logger.info("Consumer and producer closed")

if __name__ == "__main__":
    main()
