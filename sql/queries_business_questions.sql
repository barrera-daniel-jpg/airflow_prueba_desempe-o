-- =============================================================================
-- CONSULTAS DE VALIDACIÓN DE NEGOCIO - DATAMART S.A.S.
-- =============================================================================

-- 1. Evolución mensual de las ventas netas (descontando devoluciones)
WITH monthly_gross AS (
    SELECT 
        d.year, 
        d.month, 
        d.month_name,
        SUM(s.gross_revenue) AS gross_sales
    FROM dwh.fact_sales s
    JOIN dwh.dim_date d ON s.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
),
monthly_returns AS (
    SELECT 
        d.year, 
        d.month, 
        SUM(r.refund_amount) AS total_refunds
    FROM dwh.fact_returns r
    JOIN dwh.dim_date d ON r.date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT 
    g.year, 
    g.month, 
    g.month_name,
    g.gross_sales, 
    COALESCE(r.total_refunds, 0) AS returns_amount,
    (g.gross_sales - COALESCE(r.total_refunds, 0)) AS net_sales
FROM monthly_gross g
LEFT JOIN monthly_returns r ON g.year = r.year AND g.month = r.month
ORDER BY g.year, g.month;

-- 2. Categorías que generaron más revenue bruto y proporción de devoluciones
WITH cat_sales AS (
    SELECT 
        p.category, 
        SUM(s.gross_revenue) AS gross_revenue
    FROM dwh.fact_sales s
    JOIN dwh.dim_products p ON s.product_key = p.product_key
    GROUP BY p.category
),
cat_returns AS (
    SELECT 
        p.category, 
        SUM(r.refund_amount) AS refund_amount
    FROM dwh.fact_returns r
    JOIN dwh.dim_products p ON r.product_key = p.product_key
    GROUP BY p.category
)
SELECT 
    s.category,
    s.gross_revenue,
    COALESCE(r.refund_amount, 0) AS total_refunded,
    ROUND((COALESCE(r.refund_amount, 0) / NULLIF(s.gross_revenue, 0)) * 100, 2) AS return_rate_percentage
FROM cat_sales s
LEFT JOIN cat_returns r ON s.category = r.category
ORDER BY s.gross_revenue DESC;

-- 3. Top 10 productos con mayor revenue neto y Top 10 con mayor tasa de devolución
-- (A) Top 10 productos con mayor Revenue Neto:
SELECT 
    p.product_code, 
    p.canonical_description,
    (COALESCE(SUM(s.gross_revenue), 0) - COALESCE(SUM(r.refund_amount), 0)) AS net_revenue
FROM dwh.dim_products p
LEFT JOIN dwh.fact_sales s ON p.product_key = s.product_key
LEFT JOIN dwh.fact_returns r ON p.product_key = r.product_key
GROUP BY p.product_code, p.canonical_description
ORDER BY net_revenue DESC
LIMIT 10;

-- (B) Top 10 productos con mayor tasa de devolución (con al menos 20 ventas):
SELECT 
    p.product_code,
    p.canonical_description,
    SUM(s.quantity) AS total_units_sold,
    COALESCE(SUM(r.quantity_returned), 0) AS total_units_returned,
    ROUND((COALESCE(SUM(r.quantity_returned), 0)::numeric / NULLIF(SUM(s.quantity), 0)) * 100, 2) AS return_rate_pct
FROM dwh.dim_products p
JOIN dwh.fact_sales s ON p.product_key = s.product_key
LEFT JOIN dwh.fact_returns r ON p.product_key = r.product_key
GROUP BY p.product_code, p.canonical_description
HAVING SUM(s.quantity) >= 20
ORDER BY return_rate_pct DESC
LIMIT 10;

-- 4. Países que concentran las transacciones y ticket promedio
SELECT 
    country, 
    COUNT(DISTINCT invoice_no) AS total_invoices,
    SUM(gross_revenue) AS total_revenue,
    ROUND(SUM(gross_revenue) / NULLIF(COUNT(DISTINCT invoice_no), 0), 2) AS avg_ticket_value
FROM dwh.fact_sales
GROUP BY country
ORDER BY total_invoices DESC;

-- 5. Diferencia de comportamiento de compra: Clientes identificados vs anónimos
SELECT 
    c.customer_type,
    COUNT(DISTINCT s.invoice_no) AS total_orders,
    SUM(s.quantity) AS total_units_sold,
    SUM(s.gross_revenue) AS total_revenue,
    ROUND(SUM(s.gross_revenue) / NULLIF(COUNT(DISTINCT s.invoice_no), 0), 2) AS avg_ticket_by_order,
    ROUND(AVG(s.gross_revenue), 2) AS avg_line_value
FROM dwh.fact_sales s
JOIN dwh.dim_customers c ON s.customer_key = c.customer_key
GROUP BY c.customer_type;

-- 6. Total de códigos únicos de producto y verificación de consistencia
SELECT 
    COUNT(DISTINCT product_code) AS total_unique_product_codes,
    COUNT(*) AS total_registered_products
FROM dwh.dim_products;

-- 7. Métricas de auditoría del log de rechazos
SELECT 
    rejection_reason,
    source_file,
    COUNT(*) AS total_rejected_records
FROM dwh.data_quality_rejects
GROUP BY rejection_reason, source_file
ORDER BY total_rejected_records DESC;