-- RetailAnalytics SQL. Load via utils.load_sql_section("analytics.sql", "<section>").

-- @section kpis
WITH base AS (
    SELECT
        SUM(line_total) AS revenue,
        SUM(Quantity) AS units,
        COUNT(DISTINCT InvoiceNo) AS invoices,
        COUNT(DISTINCT CustomerID) AS customers,
        COUNT(DISTINCT StockCode) AS products,
        COUNT(*) AS line_items,
        COUNT(DISTINCT Country) AS countries,
        MIN(InvoiceDate) AS min_date,
        MAX(InvoiceDate) AS max_date,
        SUM(CASE WHEN Country = 'United Kingdom' THEN line_total ELSE 0 END) AS uk_revenue
    FROM stg_sales_clean
)
SELECT
    revenue,
    units,
    invoices,
    customers,
    products,
    line_items,
    countries,
    min_date,
    max_date,
    uk_revenue,
    (revenue / NULLIF(invoices, 0)) AS avg_invoice_value,
    (line_items * 1.0 / NULLIF(invoices, 0)) AS avg_lines_per_invoice,
    (revenue * 1.0 / NULLIF(customers, 0)) AS avg_spend_per_customer,
    (uk_revenue * 1.0 / NULLIF(revenue, 0)) AS uk_revenue_share
FROM base
-- @end

-- @section weekday
SELECT
    CAST(strftime('%w', InvoiceDate) AS INTEGER) AS weekday_num,
    SUM(line_total) AS revenue,
    COUNT(DISTINCT InvoiceNo) AS invoices
FROM stg_sales_clean
GROUP BY weekday_num
ORDER BY weekday_num
-- @end

-- @section invoice_distribution
SELECT
    InvoiceNo,
    SUM(line_total) AS invoice_revenue,
    SUM(Quantity) AS units,
    COUNT(DISTINCT StockCode) AS distinct_products
FROM stg_sales_clean
GROUP BY InvoiceNo
-- @end

-- @section rfm_max_date
SELECT MAX(InvoiceDate) AS max_date FROM stg_sales_clean
-- @end

-- @section rfm_customers
SELECT
    CustomerID,
    MAX(InvoiceDate) AS last_invoice,
    COUNT(DISTINCT InvoiceNo) AS frequency,
    SUM(line_total) AS monetary
FROM stg_sales_clean
GROUP BY CustomerID
-- @end
