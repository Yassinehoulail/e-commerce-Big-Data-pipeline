"""
Script de transformation Silver → Gold
Génération de statistiques analytiques et données prêtes pour l'analyse
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
        logging.FileHandler('transform_silver_to_gold.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

SILVER_PATH = "../data/silver/companies_revenue_silver.csv"
GOLD_PATH = "../data/gold/companies_revenue_gold.csv"

def load_silver_data():
    """
    Charge les données depuis la couche Silver
    """
    logger.info("Chargement des données Silver...")
    
    if not os.path.exists(SILVER_PATH):
        logger.error(f"Le fichier Silver n'existe pas: {SILVER_PATH}")
        return None
    
    df = pd.read_csv(SILVER_PATH)
    logger.info(f"Données Silver chargées: {len(df)} lignes")
    
    return df

def calculate_analytics(df):
    """
    Calculer des statistiques analytiques
    """
    logger.info("Calcul des statistiques analytiques...")
    
    # Calculer les percentiles de revenu
    df['revenue_percentile'] = df['revenue'].rank(pct=True) * 100
    
    # Calculer les catégories de revenu
    conditions = [
        df['revenue'] < df['revenue'].quantile(0.25),
        (df['revenue'] >= df['revenue'].quantile(0.25)) & (df['revenue'] < df['revenue'].quantile(0.5)),
        (df['revenue'] >= df['revenue'].quantile(0.5)) & (df['revenue'] < df['revenue'].quantile(0.75)),
        df['revenue'] >= df['revenue'].quantile(0.75)
    ]
    choices = ['Low', 'Medium-Low', 'Medium-High', 'High']
    df['revenue_category'] = np.select(conditions, choices, default='Medium')
    
    # Calculer le ratio revenue/sales
    df['revenue_per_sale'] = df['revenue'] / df['sales']
    
    # Calculer le profit estimé (20% de marge)
    df['estimated_profit'] = df['revenue'] * 0.20
    
    # Calculer le Z-score du revenu
    df['revenue_zscore'] = (df['revenue'] - df['revenue'].mean()) / df['revenue'].std()
    
    logger.info("Statistiques analytiques calculées")
    
    return df

def create_aggregated_metrics(df):
    """
    Créer des métriques agrégées par catégorie
    """
    logger.info("Création des métriques agrégées...")
    
    # Agrégation par catégorie
    category_stats = df.groupby('category').agg({
        'revenue': ['sum', 'mean', 'count'],
        'price': 'mean',
        'sales': 'sum'
    }).reset_index()
    
    category_stats.columns = ['category', 'total_revenue', 'avg_revenue', 'count', 'avg_price', 'total_sales']
    
    logger.info(f"Métriques agrégées pour {len(category_stats)} catégories")
    
    return category_stats

def add_business_metrics(df):
    """
    Ajouter des métriques business
    """
    logger.info("Ajout des métriques business...")
    
    # Calculer le rang de revenu
    df['revenue_rank'] = df['revenue'].rank(ascending=False)
    
    # Calculer le pourcentage du revenu total
    total_revenue = df['revenue'].sum()
    df['revenue_percentage'] = (df['revenue'] / total_revenue) * 100
    
    # Identifier les top performers (top 10%)
    df['is_top_performer'] = df['revenue_percentile'] >= 90
    
    # Calculer l'écart par rapport à la moyenne
    df['revenue_diff_from_avg'] = df['revenue'] - df['revenue'].mean()
    
    logger.info("Métriques business ajoutées")
    
    return df

def quality_checks_gold(df):
    """
    Contrôles qualité des données Gold
    """
    logger.info("=" * 50)
    logger.info("CONTRÔLES QUALITÉ GOLD")
    logger.info("=" * 50)
    
    # Vérifier les valeurs nulles
    null_counts = df.isnull().sum()
    logger.info(f"Valeurs nulles par colonne:\n{null_counts[null_counts > 0]}")
    
    # Vérifier les valeurs infinies
    inf_counts = np.isinf(df.select_dtypes(include=[np.number])).sum()
    logger.info(f"Valeurs infinies:\n{inf_counts[inf_counts > 0]}")
    
    # Statistiques des données Gold
    logger.info("\nStatistiques des données Gold:")
    logger.info(f"  - Total lignes: {len(df)}")
    logger.info(f"  - Revenue total: {df['revenue'].sum():.2f}")
    logger.info(f"  - Revenue moyen: {df['revenue'].mean():.2f}")
    logger.info(f"  - Profit estimé total: {df['estimated_profit'].sum():.2f}")
    logger.info(f"  - Top performers: {df['is_top_performer'].sum()}")
    logger.info(f"  - Distribution des catégories de revenu:")
    logger.info(f"    {df['revenue_category'].value_counts()}")
    
    logger.info("=" * 50)
    
    return df

def save_gold_data(df):
    """
    Sauvegarde les données dans la couche Gold
    """
    logger.info("Sauvegarde des données Gold...")
    
    # Créer le dossier si nécessaire
    os.makedirs(os.path.dirname(GOLD_PATH), exist_ok=True)
    
    df.to_csv(GOLD_PATH, index=False, encoding='utf-8')
    logger.info(f"Données Gold sauvegardées dans {GOLD_PATH}")
    logger.info(f"Total de {len(df)} lignes")

def save_gold_summary(df):
    """
    Sauvegarder un résumé des données Gold
    """
    logger.info("Sauvegarde du résumé Gold...")
    
    summary_path = "../data/gold/companies_revenue_gold_summary.csv"
    
    # Créer un résumé
    summary = pd.DataFrame({
        'metric': [
            'total_rows',
            'total_revenue',
            'avg_revenue',
            'median_revenue',
            'min_revenue',
            'max_revenue',
            'total_profit',
            'top_performers_count',
            'avg_rating',
            'total_sales'
        ],
        'value': [
            len(df),
            df['revenue'].sum(),
            df['revenue'].mean(),
            df['revenue'].median(),
            df['revenue'].min(),
            df['revenue'].max(),
            df['estimated_profit'].sum(),
            df['is_top_performer'].sum(),
            df['rating_numeric'].mean(),
            df['sales'].sum()
        ]
    })
    
    summary.to_csv(summary_path, index=False, encoding='utf-8')
    logger.info(f"Résumé Gold sauvegardé dans {summary_path}")

def main():
    """
    Fonction principale de transformation Silver → Gold
    """
    logger.info("=" * 50)
    logger.info("TRANSFORMATION SILVER → GOLD")
    logger.info("=" * 50)
    
    # Charger les données Silver
    df = load_silver_data()
    
    if df is None:
        logger.error("Impossible de charger les données Silver")
        return
    
    # Calculer les statistiques analytiques
    df = calculate_analytics(df)
    
    # Ajouter les métriques business
    df = add_business_metrics(df)
    
    # Contrôles qualité
    df = quality_checks_gold(df)
    
    # Sauvegarder les données Gold
    save_gold_data(df)
    
    # Sauvegarder le résumé
    save_gold_summary(df)
    
    logger.info("Transformation Silver → Gold terminée avec succès!")

if __name__ == "__main__":
    main()
