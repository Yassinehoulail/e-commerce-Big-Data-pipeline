"""
Script de chargement des données Gold dans MySQL
Ce script charge les données transformées depuis la couche Gold vers MySQL
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime
import os

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('load_to_mysql.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration MySQL
MYSQL_CONFIG = {
    'host': 'mysql',
    'port': 3306,
    'user': 'ecommerce_user',
    'password': 'ecommerce_password',
    'database': 'ecommerce_db'
}

GOLD_PATH = "/opt/airflow/data/gold/companies_revenue_gold.csv"

def create_connection():
    """
    Crée une connexion à la base de données MySQL
    """
    connection = None
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        logger.info("Connexion à MySQL réussie")
        return connection
    except Error as e:
        logger.error(f"Erreur lors de la connexion à MySQL: {e}")
        return None

def load_gold_data():
    """
    Charge les données depuis le fichier CSV Gold
    """
    logger.info("Chargement des données Gold depuis le CSV...")
    
    if not os.path.exists(GOLD_PATH):
        logger.error(f"Le fichier Gold n'existe pas: {GOLD_PATH}")
        return None
    
    df = pd.read_csv(GOLD_PATH)
    logger.info(f"Données Gold chargées: {len(df)} lignes")
    logger.info(f"Colonnes: {df.columns.tolist()}")
    
    return df

def clear_existing_data(connection):
    """
    Supprime les données existantes de la table books_gold
    """
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM books_gold")
        connection.commit()
        logger.info("Données existantes supprimées de la table books_gold")
        cursor.close()
    except Error as e:
        logger.error(f"Erreur lors de la suppression des données: {e}")
        connection.rollback()

def insert_gold_data(connection, df):
    """
    Insère les données Gold dans la table MySQL
    """
    try:
        cursor = connection.cursor()
        
        # Préparer la requête d'insertion
        insert_query = """
        INSERT INTO books_gold (
            title, price, rating, rating_numeric, sales, revenue, 
            category, scraped_date, revenue_percentile, revenue_category,
            revenue_per_sale, estimated_profit, revenue_zscore, revenue_rank,
            revenue_percentage, is_top_performer, revenue_diff_from_avg
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # Préparer les données
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                row['title'],
                row['price'],
                row['rating'],
                row['rating_numeric'],
                row['sales'],
                row['revenue'],
                row['category'],
                row['scraped_date'],
                row['revenue_percentile'],
                row['revenue_category'],
                row['revenue_per_sale'],
                row['estimated_profit'],
                row['revenue_zscore'],
                row['revenue_rank'],
                row['revenue_percentage'],
                bool(row['is_top_performer']),
                row['revenue_diff_from_avg']
            ))
        
        # Insérer les données par lots
        batch_size = 100
        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            logger.info(f"Lot {i//batch_size + 1} inséré: {len(batch)} lignes")
        
        logger.info(f"Total de {len(df)} lignes insérées dans la table books_gold")
        cursor.close()
        
        return True
        
    except Error as e:
        logger.error(f"Erreur lors de l'insertion des données: {e}")
        connection.rollback()
        return False

def update_analytics_summary(connection, df):
    """
    Met à jour la table analytics_summary avec les statistiques actuelles
    """
    try:
        cursor = connection.cursor()
        
        # Supprimer les anciennes statistiques
        cursor.execute("DELETE FROM analytics_summary")
        connection.commit()
        
        # Insérer les nouvelles statistiques
        stats = [
            ('total_books', int(len(df)), 'Nombre total de livres dans la base'),
            ('total_revenue', float(df['revenue'].sum()), 'Revenu total généré'),
            ('avg_revenue', float(df['revenue'].mean()), 'Revenu moyen par livre'),
            ('total_sales', int(df['sales'].sum()), 'Nombre total de ventes'),
            ('avg_price', float(df['price'].mean()), 'Prix moyen des livres'),
            ('total_profit', float(df['estimated_profit'].sum()), 'Profit total estimé'),
            ('top_performers', int(df['is_top_performer'].sum()), 'Nombre de top performers')
        ]
        
        insert_query = """
        INSERT INTO analytics_summary (metric_name, metric_value, metric_description)
        VALUES (%s, %s, %s)
        """
        
        cursor.executemany(insert_query, stats)
        connection.commit()
        
        logger.info("Statistiques analytiques mises à jour dans la table analytics_summary")
        cursor.close()
        
    except Error as e:
        logger.error(f"Erreur lors de la mise à jour des statistiques: {e}")
        connection.rollback()

def log_load_event(connection, rows_loaded, status, error_message=None):
    """
    Enregistre l'événement de chargement dans la table load_logs
    """
    try:
        cursor = connection.cursor()
        
        insert_query = """
        INSERT INTO load_logs (load_date, source_file, rows_loaded, load_status, error_message)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            datetime.now(),
            GOLD_PATH,
            rows_loaded,
            status,
            error_message
        ))
        connection.commit()
        
        logger.info("Événement de chargement enregistré dans load_logs")
        cursor.close()
        
    except Error as e:
        logger.error(f"Erreur lors de l'enregistrement du log: {e}")

def verify_data(connection):
    """
    Vérifie les données chargées dans MySQL
    """
    try:
        cursor = connection.cursor()
        
        # Compter les lignes
        cursor.execute("SELECT COUNT(*) FROM books_gold")
        count = cursor.fetchone()[0]
        logger.info(f"Nombre de lignes dans books_gold: {count}")
        
        # Vérifier les statistiques
        cursor.execute("SELECT SUM(revenue), AVG(revenue) FROM books_gold")
        stats = cursor.fetchone()
        logger.info(f"Revenu total: {stats[0]}, Revenu moyen: {stats[1]}")
        
        cursor.close()
        
    except Error as e:
        logger.error(f"Erreur lors de la vérification des données: {e}")

def main():
    """
    Fonction principale de chargement des données dans MySQL
    """
    logger.info("=" * 50)
    logger.info("CHARGEMENT DES DONNÉES GOLD DANS MYSQL")
    logger.info("=" * 50)
    
    # Charger les données Gold
    df = load_gold_data()
    
    if df is None:
        logger.error("Impossible de charger les données Gold")
        return
    
    # Créer la connexion MySQL
    connection = create_connection()
    
    if connection is None:
        logger.error("Impossible de se connecter à MySQL")
        return
    
    try:
        # Supprimer les données existantes
        clear_existing_data(connection)
        
        # Insérer les nouvelles données
        success = insert_gold_data(connection, df)
        
        if success:
            # Mettre à jour les statistiques
            update_analytics_summary(connection, df)
            
            # Vérifier les données
            verify_data(connection)
            
            # Enregistrer le log de succès
            log_load_event(connection, len(df), 'SUCCESS')
            
            logger.info("Chargement des données dans MySQL terminé avec succès!")
        else:
            # Enregistrer le log d'erreur
            log_load_event(connection, 0, 'FAILED', 'Erreur lors de l\'insertion des données')
            
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
        log_load_event(connection, 0, 'FAILED', str(e))
        
    finally:
        if connection and connection.is_connected():
            connection.close()
            logger.info("Connexion MySQL fermée")

if __name__ == "__main__":
    main()
