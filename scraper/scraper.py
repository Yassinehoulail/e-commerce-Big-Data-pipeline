"""
Script de scraping web pour books.toscrape.com
Ce script scrape les informations sur les livres et les prix (revenue simulé)
Supporte le mode batch (CSV) et le mode streaming (Kafka)
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
from datetime import datetime
import json
import os

# Kafka support
try:
    from confluent_kafka import Producer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("Warning: confluent-kafka not installed. Running in CSV-only mode.")

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

BASE_URL = "http://books.toscrape.com/"
OUTPUT_PATH = "../data/bronze/companies_revenue_bronze.csv"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_RAW = "books-raw"

def delivery_report(err, msg):
    """
    Callback function for Kafka delivery reports
    """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def create_kafka_producer():
    """
    Create and return a Kafka producer
    """
    if not KAFKA_AVAILABLE:
        return None
    
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'scraper-producer',
        'acks': 'all'
    }
    
    try:
        producer = Producer(conf)
        logger.info("Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None

def produce_to_kafka(producer, book_info):
    """
    Produce a book record to Kafka
    """
    if producer is None:
        return False
    
    try:
        # Convert to JSON and encode
        value = json.dumps(book_info, default=str).encode('utf-8')
        
        # Produce to Kafka
        producer.produce(
            KAFKA_TOPIC_RAW,
            value=value,
            callback=delivery_report
        )
        
        # Flush to ensure message is sent
        producer.flush(timeout=5)
        return True
        
    except Exception as e:
        logger.error(f"Failed to produce to Kafka: {e}")
        return False

def scrape_books(use_kafka=True):
    """
    Scrape les informations des livres depuis books.toscrape.com
    """
    logger.info("Début du scraping...")
    logger.info(f"Mode Kafka: {use_kafka}")
    
    # Create Kafka producer if enabled
    producer = None
    if use_kafka and KAFKA_AVAILABLE:
        producer = create_kafka_producer()
        if producer is None:
            logger.warning("Failed to create Kafka producer, falling back to CSV-only mode")
            use_kafka = False
    elif use_kafka and not KAFKA_AVAILABLE:
        logger.warning("Kafka not available, falling back to CSV-only mode")
        use_kafka = False
    
    books_data = []
    page = 1
    
    while True:
        logger.info(f"Scraping de la page {page}...")
        
        url = f"{BASE_URL}catalogue/page-{page}.html" if page > 1 else BASE_URL
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Trouver tous les livres sur la page
            books = soup.find_all('article', class_='product_pod')
            
            if not books:
                logger.info("Plus de livres à scraper. Fin du scraping.")
                break
            
            for book in books:
                try:
                    # Extraire les informations
                    title = book.h3.a['title']
                    price_text = book.find('p', class_='price_color').text
                    price = float(price_text.replace('£', '').replace('Â', '').strip())
                    
                    # Simuler un revenue basé sur le prix (prix * 1000 ventes aléatoires)
                    import random
                    sales = random.randint(100, 10000)
                    revenue = price * sales
                    
                    # Rating
                    rating_class = book.find('p', class_='star-rating')['class']
                    rating = rating_class[1] if len(rating_class) > 1 else 'Three'
                    
                    book_info = {
                        'title': title,
                        'price': price,
                        'rating': rating,
                        'sales': sales,
                        'revenue': round(revenue, 2),
                        'category': 'Books',  # Catégorie par défaut
                        'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    # Always append to books_data for CSV fallback
                    books_data.append(book_info)
                    
                    # Produce to Kafka if enabled
                    if use_kafka:
                        if produce_to_kafka(producer, book_info):
                            logger.info(f"Livre scrapé et envoyé à Kafka: {title} - Revenue: {revenue:.2f}")
                        else:
                            logger.warning(f"Failed to send to Kafka, keeping in CSV: {title}")
                    
                    logger.info(f"Livre scrapé: {title} - Revenue: {revenue:.2f}")
                    
                except Exception as e:
                    logger.error(f"Erreur lors de l'extraction d'un livre: {e}")
                    continue
            
            page += 1
            time.sleep(1)  # Respecter le site
            
        except requests.RequestException as e:
            logger.error(f"Erreur lors de la requête: {e}")
            break
        except Exception as e:
            logger.error(f"Erreur inattendue: {e}")
            break
    
    # Close producer if exists
    if producer:
        producer.flush()
        logger.info("Kafka producer flushed and closed")
    
    return books_data

def save_to_csv(data, output_path):
    """
    Sauvegarde les données dans un fichier CSV
    """
    if not data:
        logger.warning("Aucune donnée à sauvegarder")
        return
    
    df = pd.DataFrame(data)
    
    # Créer le dossier si nécessaire
    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Données sauvegardées dans {output_path}")
    logger.info(f"Total de {len(df)} livres scrapés")
    logger.info(f"Statistiques des revenus:")
    logger.info(f"  - Revenue total: {df['revenue'].sum():.2f}")
    logger.info(f"  - Revenue moyen: {df['revenue'].mean():.2f}")
    logger.info(f"  - Revenue min: {df['revenue'].min():.2f}")
    logger.info(f"  - Revenue max: {df['revenue'].max():.2f}")

def main(use_kafka=True):
    """
    Fonction principale
    """
    logger.info("=" * 50)
    logger.info("SCRAPER BOOKS.TOSCRAPE.COM")
    logger.info("=" * 50)
    
    # Scraper les données
    books_data = scrape_books(use_kafka=use_kafka)
    
    # Sauvegarder en CSV (toujours comme fallback)
    save_to_csv(books_data, OUTPUT_PATH)
    
    logger.info("Scraping terminé avec succès!")

if __name__ == "__main__":
    import sys
    # Check for command line argument to disable Kafka
    use_kafka = True
    if len(sys.argv) > 1 and sys.argv[1] == '--no-kafka':
        use_kafka = False
    main(use_kafka=use_kafka)
