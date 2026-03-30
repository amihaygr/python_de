SELECT
    InvoiceNo,
    SUM(line_total) AS invoice_revenue,
    SUM(Quantity) AS units,
    COUNT(DISTINCT StockCode) AS distinct_products
FROM stg_sales_clean
GROUP BY InvoiceNo

