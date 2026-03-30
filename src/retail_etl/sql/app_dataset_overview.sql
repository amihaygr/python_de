SELECT
    COUNT(*) AS rows_count,
    COUNT(DISTINCT CustomerID) AS customers,
    COUNT(DISTINCT Country) AS countries,
    MIN(InvoiceDate) AS min_date,
    MAX(InvoiceDate) AS max_date
FROM stg_sales_clean
