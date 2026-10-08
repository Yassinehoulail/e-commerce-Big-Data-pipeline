"""
Consumer Bronze → Silver
Consomme les messages bruts depuis books-raw, applique le nettoyage,
et produit vers books-silver
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
        logging.FileHandler('consumer_bronze_to_silver.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_GROUP_ID = "bronze-to-silver-consumer-group"
KAFKA_TOPIC_RAW = "books-raw"
KAFKA_TOPIC_SILVER = "books-silver"

# Configuration CSV
BRONZE_CSV_PATH = "../data/bronze/companies_revenue_bronze.csv"
SILVER_CSV_PATH = "../data/silver/companies_revenue_silver.csv"

# In-memory state for deduplication and statistics
seen_titles = set()
silver_records = []
batch_size = 100

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
        consumer.subscribe([KAFKA_TOPIC_RAW])
        logger.info(f"Kafka consumer created and subscribed to {KAFKA_TOPIC_RAW}")
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
        'client.id': 'bronze-to-silver-producer',
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

def clean_revenue(record):
    """
    Nettoyage de la colonne Revenue
    """
    try:
        revenue = float(record.get('revenue', 0))
        if revenue < 0:
            revenue = abs(revenue)
        record['revenue'] = round(revenue, 2)
    except (ValueError, TypeError):
        # Use median fallback (will be calculated from batch)
        record['revenue'] = 0.0
    
    return record

def handle_null_values(record):
    """
    Gestion des valeurs nulles
    """
    # Handle price
    if record.get('price') is None or record['price'] == '':
        record['price'] = 0.0
    else:
        try:
            record['price'] = float(record['price'])
        except (ValueError, TypeError):
            record['price'] = 0.0
    
    # Handle rating
    if record.get('rating') is None or record['rating'] == '':
        record['rating'] = 'Three'
    
    # Handle category
    if record.get('category') is None or record['category'] == '':
        record['category'] = 'Unknown'
    
    # Handle sales
    if record.get('sales') is None or record['sales'] == '':
        record['sales'] = 0
    else:
        try:
            record['sales'] = int(record['sales'])
        except (ValueError, TypeError):
            record['sales'] = 0
    
    return record

def standardize_columns(record):
    """
    Standardisation des colonnes
    """
    # Normalize title
    if record.get('title'):
        record['title'] = str(record['title']).strip().title()
    
    # Normalize category
    if record.get('category'):
        record['category'] = str(record['category']).strip().capitalize()
    
    # Normalize rating to numeric
    rating_mapping = {
        'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5
    }
    rating = record.get('rating', 'Three')
    record['rating_numeric'] = rating_mapping.get(rating, 3)
    
    return record

def apply_cleaning(record):
    """
    Apply all cleaning transformations to a record
    """
    try:
        # Clean revenue
        record = clean_revenue(record)
        
        # Handle null values
        record = handle_null_values(record)
        
        # Standardize columns
        record = standardize_columns(record)
        
        return record
    except Exception as e:
        logger.error(f"Error cleaning record: {e}")
        return None

def produce_to_silver(producer, record):
    """
    Produce a cleaned record to the silver topic
    """
    try:
        value = json.dumps(record, default=str).encode('utf-8')
        producer.produce(
            KAFKA_TOPIC_SILVER,
            value=value,
            callback=delivery_report
        )
        return True
    except Exception as e:
        logger.error(f"Failed to produce to silver topic: {e}")
        return False

def append_to_bronze_csv(record):
    """
    Append raw record to bronze CSV for traceability
    """
    try:
        os.makedirs(os.path.dirname(BRONZE_CSV_PATH), exist_ok=True)
        
        # Check if file exists to determine if we need headers
        file_exists = os.path.exists(BRONZE_CSV_PATH)
        
        df = pd.DataFrame([record])
        df.to_csv(BRONZE_CSV_PATH, mode='a', header=not file_exists, index=False, encoding='utf-8')
        
        # Sync to MinIO (non-blocking, wrapped in try/except)
        if MINIO_AVAILABLE:
            try:
                object_name = os.path.basename(BRONZE_CSV_PATH)
                upload_file("bronze", object_name, BRONZE_CSV_PATH)
            except Exception as e:
                logger.warning(f"Failed to sync bronze CSV to MinIO: {e}")
        
    except Exception as e:
        logger.error(f"Failed to append to bronze CSV: {e}")

def append_to_silver_csv(records):
    """
    Append cleaned records to silver CSV
    """
    try:
        if not records:
            return
        
        os.makedirs(os.path.dirname(SILVER_CSV_PATH), exist_ok=True)
        
        # Check if file exists to determine if we need headers
        file_exists = os.path.exists(SILVER_CSV_PATH)
        
        df = pd.DataFrame(records)
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

def process_message(producer, msg):
    """
    Process a single Kafka message
    """
    try:
        # Deserialize JSON
        record = json.loads(msg.value().decode('utf-8'))
        
        # Check for duplicates
        title = record.get('title')
        if title in seen_titles:
            logger.debug(f"Duplicate record skipped: {title}")
            return
        
        seen_titles.add(title)
        
        # Apply cleaning transformations
        cleaned_record = apply_cleaning(record)
        
        if cleaned_record is None:
            logger.error("Failed to clean record, skipping")
            return
        
        # Append to bronze CSV for traceability
        append_to_bronze_csv(record)
        
        # Produce to silver topic
        if produce_to_silver(producer, cleaned_record):
            silver_records.append(cleaned_record)
            logger.info(f"Processed and sent to silver: {title}")
        
        # Flush batch if size reached
        if len(silver_records) >= batch_size:
            producer.flush()
            append_to_silver_csv(silver_records)
            logger.info(f"Batch of {len(silver_records)} records appended to silver CSV")
            silver_records.clear()
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main():
    """
    Main consumer loop
    """
    logger.info("=" * 60)
    logger.info("CONSUMER BRONZE → SILVER")
    logger.info("=" * 60)
    
    # Create consumer and producer
    consumer = create_consumer()
    producer = create_producer()
    
    if consumer is None or producer is None:
        logger.error("Failed to initialize consumer or producer")
        return
    
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
            process_message(producer, msg)
            
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Flush remaining records
        if silver_records:
            producer.flush()
            append_to_silver_csv(silver_records)
            logger.info(f"Flushed final batch of {len(silver_records)} records")
        
        # Close consumer and producer
        consumer.close()
        producer.flush()
        logger.info("Consumer and producer closed")

if __name__ == "__main__":
    main()
