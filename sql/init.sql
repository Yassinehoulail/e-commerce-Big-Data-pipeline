-- Script d'initialisation de la base de données MySQL
-- Ce script est exécuté automatiquement au démarrage du conteneur MySQL

-- Création de la base de données si elle n'existe pas
CREATE DATABASE IF NOT EXISTS ecommerce_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Utilisation de la base de données
USE ecommerce_db;

-- Table pour les données Gold (livres avec analytics)
CREATE TABLE IF NOT EXISTS books_gold (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    rating VARCHAR(50),
    rating_numeric INT,
    sales INT NOT NULL,
    revenue DECIMAL(15, 2) NOT NULL,
    category VARCHAR(100),
    scraped_date DATETIME,
    revenue_percentile DECIMAL(5, 2),
    revenue_category VARCHAR(50),
    revenue_per_sale DECIMAL(10, 2),
    estimated_profit DECIMAL(15, 2),
    revenue_zscore DECIMAL(10, 2),
    revenue_rank INT,
    revenue_percentage DECIMAL(5, 2),
    is_top_performer BOOLEAN DEFAULT FALSE,
    revenue_diff_from_avg DECIMAL(15, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_title (title),
    INDEX idx_revenue (revenue),
    INDEX idx_category (category),
    INDEX idx_rating (rating_numeric)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Table pour les statistiques globales
CREATE TABLE IF NOT EXISTS analytics_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(20, 2),
    metric_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Table pour les logs de chargement
CREATE TABLE IF NOT EXISTS load_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    load_date DATETIME NOT NULL,
    source_file VARCHAR(255),
    rows_loaded INT,
    load_status VARCHAR(50),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insertion des données initiales de statistiques
INSERT INTO analytics_summary (metric_name, metric_value, metric_description) VALUES
('total_books', 0, 'Nombre total de livres dans la base'),
('total_revenue', 0, 'Revenu total généré'),
('avg_revenue', 0, 'Revenu moyen par livre'),
('total_sales', 0, 'Nombre total de ventes'),
('avg_price', 0, 'Prix moyen des livres');

-- Affichage des informations
SELECT 'Base de données ecommerce_db créée avec succès!' AS message;
SELECT 'Tables créées: books_gold, analytics_summary, load_logs' AS tables;
