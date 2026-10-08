"""
Consumer Gold → Database
Consomme les messages enrichis depuis books-gold et insère dans MySQL
"""

import json
import logging
import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Kafka support
try:
    from confluent_kafka import Consumer
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
        logging.FileHandler('consumer_gold_to_db.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_GROUP_ID = "gold-to-db-consumer-group"
KAFKA_TOPIC_GOLD = "books-gold"

# Configuration MySQL
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'ecommerce_user',
    'password': 'ecommerce_password',
    'database': 'ecommerce_db'
}

# Configuration CSV
GOLD_CSV_PATH = "../data/gold/companies_revenue_gold.csv"

# Statistics
records_processed = 0
records_inserted = 0
records_failed = 0

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
        consumer.subscribe([KAFKA_TOPIC_GOLD])
        logger.info(f"Kafka consumer created and subscribed to {KAFKA_TOPIC_GOLD}")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        return None

def create_mysql_connection():
    """
    Create and return a MySQL connection
    """
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        logger.info("MySQL connection established")
        return connection
    except Error as e:
        logger.error(f"Failed to connect to MySQL: {e}")
        return None

def insert_record_to_db(connection, record):
    """
    Insert a single record into the books_gold table
    Uses UPSERT (INSERT ... ON DUPLICATE KEY UPDATE) to handle duplicates
    """
    try:
        cursor = connection.cursor()
        
        # Prepare the insert query with upsert
        insert_query = """
        INSERT INTO books_gold (
            title, price, rating, rating_numeric, sales, revenue, 
            category, scraped_date, revenue_percentile, revenue_category,
            revenue_per_sale, estimated_profit, revenue_zscore, revenue_rank,
            revenue_percentage, is_top_performer, revenue_diff_from_avg
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON DUPLICATE KEY UPDATE
            price = VALUES(price),
            rating = VALUES(rating),
            rating_numeric = VALUES(rating_numeric),
            sales = VALUES(sales),
            revenue = VALUES(revenue),
            category = VALUES(category),
            scraped_date = VALUES(scraped_date),
            revenue_percentile = VALUES(revenue_percentile),
            revenue_category = VALUES(revenue_category),
            revenue_per_sale = VALUES(revenue_per_sale),
            estimated_profit = VALUES(estimated_profit),
            revenue_zscore = VALUES(revenue_zscore),
            revenue_rank = VALUES(revenue_rank),
            revenue_percentage = VALUES(revenue_percentage),
            is_top_performer = VALUES(is_top_performer),
            revenue_diff_from_avg = VALUES(revenue_diff_from_avg)
        """
        
        # Prepare the data
        data = (
            record.get('title'),
            float(record.get('price', 0)),
            record.get('rating'),
            int(record.get('rating_numeric', 3)),
            int(record.get('sales', 0)),
            float(record.get('revenue', 0)),
            record.get('category'),
            record.get('scraped_date'),
            float(record.get('revenue_percentile', 50)),
            record.get('revenue_category'),
            float(record.get('revenue_per_sale', 0)),
            float(record.get('estimated_profit', 0)),
            float(record.get('revenue_zscore', 0)),
            int(record.get('revenue_rank', 0)),
            float(record.get('revenue_percentage', 0)),
            bool(record.get('is_top_performer', False)),
            float(record.get('revenue_diff_from_avg', 0))
        )
        
        cursor.execute(insert_query, data)
        connection.commit()
        cursor.close()
        
        return True
        
    except Error as e:
        logger.error(f"Database error: {e}")
        connection.rollback()
        return False
    except Exception as e:
        logger.error(f"Unexpected error inserting record: {e}")
        connection.rollback()
        return False

def append_to_gold_csv(record):
    """
    Append gold record to CSV for traceability
    """
    try:
        os.makedirs(os.path.dirname(GOLD_CSV_PATH), exist_ok=True)
        
        file_exists = os.path.exists(GOLD_CSV_PATH)
        
        df = pd.DataFrame([record])
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

def log_load_event(connection, status, error_message=None):
    """
    Log a load event to the load_logs table
    """
    try:
        cursor = connection.cursor()
        
        insert_query = """
        INSERT INTO load_logs (load_date, source_file, rows_loaded, load_status, error_message)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            datetime.now(),
            'kafka://books-gold',
            records_processed,
            status,
            error_message
        ))
        connection.commit()
        cursor.close()
        
    except Error as e:
        logger.error(f"Failed to log load event: {e}")

def process_message(connection, msg):
    """
    Process a single Kafka message
    """
    global records_processed, records_inserted, records_failed
    
    try:
        # Deserialize JSON
        record = json.loads(msg.value().decode('utf-8'))
        
        records_processed += 1
        
        # Append to gold CSV for traceability
        append_to_gold_csv(record)
        
        # Insert into database
        if insert_record_to_db(connection, record):
            records_inserted += 1
            logger.info(f"Record inserted into DB: {record.get('title')}")
        else:
            records_failed += 1
            logger.error(f"Failed to insert record: {record.get('title')}")
        
        # Log progress every 100 records
        if records_processed % 100 == 0:
            logger.info(f"Progress: {records_processed} processed, {records_inserted} inserted, {records_failed} failed")
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON: {e}")
        records_failed += 1
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        records_failed += 1

def main():
    """
    Main consumer loop
    """
    global records_processed, records_inserted, records_failed
    
    logger.info("=" * 60)
    logger.info("CONSUMER GOLD → DATABASE")
    logger.info("=" * 60)
    
    # Create consumer
    consumer = create_consumer()
    
    if consumer is None:
        logger.error("Failed to initialize consumer")
        return
    
    # Create MySQL connection
    connection = create_mysql_connection()
    
    if connection is None:
        logger.error("Failed to connect to MySQL")
        consumer.close()
        return
    
    try:
        logger.info("Starting consumer loop...")
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # No message, continue
                continue
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            # Process the message
            process_message(connection, msg)
            
            # Reconnect to MySQL if connection is lost
            if not connection.is_connected():
                logger.warning("MySQL connection lost, attempting to reconnect...")
                connection.close()
                connection = create_mysql_connection()
                if connection is None:
                    logger.error("Failed to reconnect to MySQL, waiting...")
                    import time
                    time.sleep(5)
            
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Log final statistics
        if connection and connection.is_connected():
            log_load_event(connection, 'SUCCESS' if records_failed == 0 else 'PARTIAL')
            logger.info("=" * 60)
            logger.info("FINAL STATISTICS")
            logger.info("=" * 60)
            logger.info(f"Records processed: {records_processed}")
            logger.info(f"Records inserted: {records_inserted}")
            logger.info(f"Records failed: {records_failed}")
            logger.info("=" * 60)
            connection.close()
        
        # Close consumer
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    main()
