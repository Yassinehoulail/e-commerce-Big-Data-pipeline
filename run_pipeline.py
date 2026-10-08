"""
Script principal pour exécuter tout le pipeline de données
Ce script exécute: Scraping → Bronze→Silver → Silver→Gold → Chargement MySQL
"""

import subprocess
import sys
import os
from datetime import datetime

def run_command(command, description):
    """
    Exécute une commande et affiche le résultat
    """
    print("=" * 60)
    print(f"{description}")
    print("=" * 60)
    print(f"Commande: {command}")
    print("-" * 60)
    
    result = subprocess.run(command, shell=True, capture_output=False)
    
    if result.returncode == 0:
        print(f"✓ {description} terminé avec succès")
    else:
        print(f"✗ {description} a échoué")
        return False
    
    return True

def main():
    """
    Fonction principale pour exécuter tout le pipeline
    """
    print("\n" + "=" * 60)
    print("PIPELINE COMPLET DE DONNÉES E-COMMERCE")
    print("Architecture Médaillon: Bronze → Silver → Gold → MySQL")
    print("=" * 60 + "\n")
    
    start_time = datetime.now()
    
    # Étape 1: Scraping (Bronze)
    if not run_command(
        "cd scraper && python scraper.py",
        "ÉTAPE 1: Scraping Web (Bronze)"
    ):
        print("\n❌ Pipeline arrêté: Erreur lors du scraping")
        sys.exit(1)
    
    # Étape 2: Transformation Bronze → Silver
    if not run_command(
        "cd transform && python bronze_to_silver.py",
        "ÉTAPE 2: Transformation Bronze → Silver"
    ):
        print("\n❌ Pipeline arrêté: Erreur lors de la transformation Bronze→Silver")
        sys.exit(1)
    
    # Étape 3: Transformation Silver → Gold
    if not run_command(
        "cd transform && python silver_to_gold.py",
        "ÉTAPE 3: Transformation Silver → Gold"
    ):
        print("\n❌ Pipeline arrêté: Erreur lors de la transformation Silver→Gold")
        sys.exit(1)
    
    # Étape 4: Chargement dans MySQL
    if not run_command(
        "python load_to_mysql.py",
        "ÉTAPE 4: Chargement des données Gold dans MySQL"
    ):
        print("\n❌ Pipeline arrêté: Erreur lors du chargement MySQL")
        sys.exit(1)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 60)
    print("🎉 PIPELINE TERMINÉ AVEC SUCCÈS!")
    print("=" * 60)
    print(f"Durée totale: {duration}")
    print("\n📊 Données disponibles:")
    print("  - data/bronze/companies_revenue_bronze.csv")
    print("  - data/silver/companies_revenue_silver.csv")
    print("  - data/gold/companies_revenue_gold.csv")
    print("  - data/gold/companies_revenue_gold_summary.csv")
    print("\n🗄️  Base de données MySQL:")
    print("  - Base: ecommerce_db")
    print("  - Table: books_gold")
    print("  - Table: analytics_summary")
    print("\n🔗 Accès phpMyAdmin: http://localhost:8080")
    print("  - User: root")
    print("  - Password: rootpassword")
    print("\n📝 Logs disponibles:")
    print("  - scraper/scraper.log")
    print("  - transform/transform_bronze_to_silver.log")
    print("  - transform/transform_silver_to_gold.log")
    print("  - load_to_mysql.log")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    main()
