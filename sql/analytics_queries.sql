-- Requêtes SQL analytiques pour le projet E-commerce Big Data
-- Ces requêtes permettent d'analyser les données de la couche Gold

-- ============================================
-- 1. TOP ENTREPRISES PAR REVENUE
-- ============================================

-- Top 10 livres par revenu
SELECT 
    title,
    revenue,
    sales,
    price,
    rating,
    revenue_category,
    revenue_rank
FROM books_gold
ORDER BY revenue DESC
LIMIT 10;

-- Top 20 livres par revenu avec plus de détails
SELECT 
    title,
    revenue,
    sales,
    price,
    rating_numeric,
    revenue_category,
    revenue_percentile,
    is_top_performer,
    estimated_profit,
    revenue_percentage
FROM books_gold
ORDER BY revenue DESC
LIMIT 20;

-- ============================================
-- 2. STATISTIQUES GLOBALES
-- ============================================

-- Statistiques globales des revenus
SELECT 
    COUNT(*) AS total_books,
    SUM(revenue) AS total_revenue,
    AVG(revenue) AS avg_revenue,
    MIN(revenue) AS min_revenue,
    MAX(revenue) AS max_revenue,
    STDDEV(revenue) AS std_revenue,
    MEDIAN(revenue) AS median_revenue
FROM books_gold;

-- Statistiques des ventes
SELECT 
    COUNT(*) AS total_books,
    SUM(sales) AS total_sales,
    AVG(sales) AS avg_sales,
    MIN(sales) AS min_sales,
    MAX(sales) AS max_sales
FROM books_gold;

-- Statistiques des prix
SELECT 
    COUNT(*) AS total_books,
    AVG(price) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    STDDEV(price) AS std_price
FROM books_gold;

-- ============================================
-- 3. REVENUE MOYEN
-- ============================================

-- Revenue moyen par catégorie
SELECT 
    category,
    COUNT(*) AS book_count,
    AVG(revenue) AS avg_revenue,
    SUM(revenue) AS total_revenue,
    AVG(price) AS avg_price,
    AVG(sales) AS avg_sales
FROM books_gold
GROUP BY category
ORDER BY avg_revenue DESC;

-- Revenue moyen par rating
SELECT 
    rating,
    rating_numeric,
    COUNT(*) AS book_count,
    AVG(revenue) AS avg_revenue,
    SUM(revenue) AS total_revenue,
    AVG(price) AS avg_price
FROM books_gold
GROUP BY rating, rating_numeric
ORDER BY rating_numeric DESC;

-- ============================================
-- 4. TOTAL REVENUE
-- ============================================

-- Total revenue par catégorie
SELECT 
    category,
    COUNT(*) AS book_count,
    SUM(revenue) AS total_revenue,
    ROUND(SUM(revenue) / (SELECT SUM(revenue) FROM books_gold) * 100, 2) AS revenue_percentage,
    AVG(revenue) AS avg_revenue
FROM books_gold
GROUP BY category
ORDER BY total_revenue DESC;

-- Total revenue par catégorie de revenu
SELECT 
    revenue_category,
    COUNT(*) AS book_count,
    SUM(revenue) AS total_revenue,
    AVG(revenue) AS avg_revenue,
    MIN(revenue) AS min_revenue,
    MAX(revenue) AS max_revenue
FROM books_gold
GROUP BY revenue_category
ORDER BY 
    CASE revenue_category
        WHEN 'High' THEN 1
        WHEN 'Medium-High' THEN 2
        WHEN 'Medium-Low' THEN 3
        WHEN 'Low' THEN 4
    END;

-- ============================================
-- 5. ANALYSE DES TOP PERFORMERS
-- ============================================

-- Statistiques des top performers
SELECT 
    COUNT(*) AS top_performers_count,
    SUM(revenue) AS total_revenue,
    AVG(revenue) AS avg_revenue,
    ROUND(SUM(revenue) / (SELECT SUM(revenue) FROM books_gold) * 100, 2) AS revenue_percentage
FROM books_gold
WHERE is_top_performer = TRUE;

-- Top performers détaillés
SELECT 
    title,
    revenue,
    sales,
    price,
    rating,
    revenue_category,
    revenue_percentile,
    estimated_profit
FROM books_gold
WHERE is_top_performer = TRUE
ORDER BY revenue DESC;

-- ============================================
-- 6. ANALYSE PAR CATÉGORIE
-- ============================================

-- Distribution des livres par catégorie
SELECT 
    category,
    COUNT(*) AS book_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM books_gold), 2) AS percentage
FROM books_gold
GROUP BY category
ORDER BY book_count DESC;

-- Performance par catégorie
SELECT 
    category,
    COUNT(*) AS book_count,
    SUM(revenue) AS total_revenue,
    AVG(revenue) AS avg_revenue,
    AVG(price) AS avg_price,
    SUM(sales) AS total_sales,
    AVG(sales) AS avg_sales,
    SUM(estimated_profit) AS total_profit
FROM books_gold
GROUP BY category
ORDER BY total_revenue DESC;

-- ============================================
-- 7. ANALYSE DES RATINGS
-- ============================================

-- Distribution des ratings
SELECT 
    rating,
    rating_numeric,
    COUNT(*) AS book_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM books_gold), 2) AS percentage,
    AVG(revenue) AS avg_revenue,
    AVG(price) AS avg_price
FROM books_gold
GROUP BY rating, rating_numeric
ORDER BY rating_numeric DESC;

-- Corrélation entre rating et revenue
SELECT 
    rating,
    rating_numeric,
    COUNT(*) AS book_count,
    AVG(revenue) AS avg_revenue,
    MIN(revenue) AS min_revenue,
    MAX(revenue) AS max_revenue
FROM books_gold
GROUP BY rating, rating_numeric
ORDER BY avg_revenue DESC;

-- ============================================
-- 8. ANALYSE DES OUTLIERS
-- ============================================

-- Livres avec des revenus exceptionnellement élevés (Z-score > 2)
SELECT 
    title,
    revenue,
    revenue_zscore,
    sales,
    price,
    rating,
    category
FROM books_gold
WHERE revenue_zscore > 2
ORDER BY revenue_zscore DESC;

-- Livres avec des revenus exceptionnellement bas (Z-score < -2)
SELECT 
    title,
    revenue,
    revenue_zscore,
    sales,
    price,
    rating,
    category
FROM books_gold
WHERE revenue_zscore < -2
ORDER BY revenue_zscore ASC;

-- ============================================
-- 9. ANALYSE DE RENTABILITÉ
-- ============================================

-- Meilleur ratio revenue/sales
SELECT 
    title,
    revenue,
    sales,
    revenue_per_sale,
    price,
    estimated_profit,
    ROUND(estimated_profit / revenue * 100, 2) AS profit_margin
FROM books_gold
ORDER BY revenue_per_sale DESC
LIMIT 10;

-- Total profit estimé par catégorie
SELECT 
    category,
    COUNT(*) AS book_count,
    SUM(estimated_profit) AS total_profit,
    AVG(estimated_profit) AS avg_profit,
    ROUND(AVG(estimated_profit / revenue * 100), 2) AS avg_profit_margin
FROM books_gold
GROUP BY category
ORDER BY total_profit DESC;

-- ============================================
-- 10. RAPPORT COMPLET
-- ============================================

-- Rapport synthétique global
SELECT 
    'Total Books' AS metric,
    COUNT(*) AS value
FROM books_gold
UNION ALL
SELECT 
    'Total Revenue',
    SUM(revenue)
FROM books_gold
UNION ALL
SELECT 
    'Average Revenue',
    AVG(revenue)
FROM books_gold
UNION ALL
SELECT 
    'Total Sales',
    SUM(sales)
FROM books_gold
UNION ALL
SELECT 
    'Average Price',
    AVG(price)
FROM books_gold
UNION ALL
SELECT 
    'Total Profit',
    SUM(estimated_profit)
FROM books_gold
UNION ALL
SELECT 
    'Top Performers',
    COUNT(*)
FROM books_gold
WHERE is_top_performer = TRUE
UNION ALL
SELECT 
    'Categories',
    COUNT(DISTINCT category)
FROM books_gold;
