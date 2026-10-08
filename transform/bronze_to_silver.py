"""
Script de transformation Bronze → Silver
Nettoyage des données brutes scrapées
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
import os

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transform_bronze_to_silver.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

BRONZE_PATH = "../data/bronze/companies_revenue_bronze.csv"
SILVER_PATH = "../data/silver/companies_revenue_silver.csv"

def load_bronze_data():
    """
    Charge les données depuis la couche Bronze
    """
    logger.info("Chargement des données Bronze...")
    
    if not os.path.exists(BRONZE_PATH):
        logger.error(f"Le fichier Bronze n'existe pas: {BRONZE_PATH}")
        return None
    
    df = pd.read_csv(BRONZE_PATH)
    logger.info(f"Données Bronze chargées: {len(df)} lignes")
    logger.info(f"Colonnes: {df.columns.tolist()}")
    
    return df

def clean_revenue_column(df):
    """
    Nettoyage de la colonne Revenue
    - Suppression des symboles de devise
    - Conversion en numérique
    - Gestion des valeurs nulles
    """
    logger.info("Nettoyage de la colonne revenue...")
    
    # Vérifier les valeurs nulles avant nettoyage
    null_count_before = df['revenue'].isnull().sum()
    logger.info(f"Valeurs nulles avant nettoyage: {null_count_before}")
    
    # S'assurer que revenue est numérique
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
    
    # Remplacer les valeurs nulles par la médiane
    median_revenue = df['revenue'].median()
    df['revenue'] = df['revenue'].fillna(median_revenue)
    
    # Vérifier les revenus négatifs
    negative_count = (df['revenue'] < 0).sum()
    logger.warning(f"Revenus négatifs détectés: {negative_count}")
    
    # Remplacer les revenus négatifs par leur valeur absolue
    df['revenue'] = df['revenue'].abs()
    
    # Arrondir à 2 décimales
    df['revenue'] = df['revenue'].round(2)
    
    null_count_after = df['revenue'].isnull().sum()
    logger.info(f"Valeurs nulles après nettoyage: {null_count_after}")
    
    return df

def remove_duplicates(df):
    """
    Suppression des doublons
    """
    logger.info("Suppression des doublons...")
    
    duplicates_before = df.duplicated().sum()
    logger.info(f"Doublons détectés: {duplicates_before}")
    
    # Supprimer les doublons basés sur le titre
    df = df.drop_duplicates(subset=['title'], keep='first')
    
    duplicates_after = df.duplicated().sum()
    logger.info(f"Doublons après suppression: {duplicates_after}")
    
    return df

def handle_null_values(df):
    """
    Gestion des valeurs nulles dans toutes les colonnes
    """
    logger.info("Gestion des valeurs nulles...")
    
    # Vérifier les valeurs nulles par colonne
    null_counts = df.isnull().sum()
    logger.info(f"Valeurs nulles par colonne:\n{null_counts}")
    
    # Remplacer les valeurs nulles dans price
    if df['price'].isnull().any():
        median_price = df['price'].median()
        df['price'] = df['price'].fillna(median_price)
        logger.info(f"Valeurs nulles dans price remplacées par la médiane: {median_price}")
    
    # Remplacer les valeurs nulles dans rating
    if df['rating'].isnull().any():
        df['rating'] = df['rating'].fillna('Three')
        logger.info("Valeurs nulles dans rating remplacées par 'Three'")
    
    # Remplacer les valeurs nulles dans category
    if df['category'].isnull().any():
        df['category'] = df['category'].fillna('Unknown')
        logger.info("Valeurs nulles dans category remplacées par 'Unknown'")
    
    return df

def standardize_columns(df):
    """
    Standardisation des colonnes
    - Normalisation des textes
    - Conversion des types
    """
    logger.info("Standardisation des colonnes...")
    
    # Normaliser les titres (majuscule pour la première lettre)
    df['title'] = df['title'].str.strip().str.title()
    
    # Normaliser les catégories
    df['category'] = df['category'].str.strip().str.capitalize()
    
    # Normaliser les ratings
    rating_mapping = {
        'One': 1,
        'Two': 2,
        'Three': 3,
        'Four': 4,
        'Five': 5
    }
    df['rating_numeric'] = df['rating'].map(rating_mapping)
    df['rating_numeric'] = df['rating_numeric'].fillna(3)
    
    logger.info("Colonnes standardisées")
    
    return df

def quality_checks(df):
    """
    Contrôles qualité des données
    """
    logger.info("=" * 50)
    logger.info("CONTRÔLES QUALITÉ")
    logger.info("=" * 50)
    
    # Vérifier les revenus manquants
    missing_revenue = df['revenue'].isnull().sum()
    logger.info(f"✓ Revenus manquants: {missing_revenue}")
    
    # Vérifier les revenus négatifs
    negative_revenue = (df['revenue'] < 0).sum()
    logger.info(f"✓ Revenus négatifs: {negative_revenue}")
    
    # Vérifier les doublons
    duplicates = df.duplicated(subset=['title']).sum()
    logger.info(f"✓ Lignes dupliquées: {duplicates}")
    
    # Vérifier les prix négatifs
    negative_price = (df['price'] < 0).sum()
    logger.info(f"✓ Prix négatifs: {negative_price}")
    
    # Statistiques globales
    logger.info("\nStatistiques des données Silver:")
    logger.info(f"  - Total lignes: {len(df)}")
    logger.info(f"  - Revenue total: {df['revenue'].sum():.2f}")
    logger.info(f"  - Revenue moyen: {df['revenue'].mean():.2f}")
    logger.info(f"  - Revenue min: {df['revenue'].min():.2f}")
    logger.info(f"  - Revenue max: {df['revenue'].max():.2f}")
    logger.info(f"  - Prix moyen: {df['price'].mean():.2f}")
    
    logger.info("=" * 50)
    
    return df

def save_silver_data(df):
    """
    Sauvegarde les données dans la couche Silver
    """
    logger.info("Sauvegarde des données Silver...")
    
    # Créer le dossier si nécessaire
    os.makedirs(os.path.dirname(SILVER_PATH), exist_ok=True)
    
    df.to_csv(SILVER_PATH, index=False, encoding='utf-8')
    logger.info(f"Données Silver sauvegardées dans {SILVER_PATH}")
    logger.info(f"Total de {len(df)} lignes")

def main():
    """
    Fonction principale de transformation Bronze → Silver
    """
    logger.info("=" * 50)
    logger.info("TRANSFORMATION BRONZE → SILVER")
    logger.info("=" * 50)
    
    # Charger les données Bronze
    df = load_bronze_data()
    
    if df is None:
        logger.error("Impossible de charger les données Bronze")
        return
    
    # Nettoyage des données
    df = clean_revenue_column(df)
    df = remove_duplicates(df)
    df = handle_null_values(df)
    df = standardize_columns(df)
    
    # Contrôles qualité
    df = quality_checks(df)
    
    # Sauvegarder les données Silver
    save_silver_data(df)
    
    logger.info("Transformation Bronze → Silver terminée avec succès!")

if __name__ == "__main__":
    main()
