SELECT
    strftime('%Y-%m', InvoiceDate) AS year_month,
    SUM(line_total) AS revenue,
    SUM(Quantity) AS units,
    COUNT(DISTINCT InvoiceNo) AS invoices
FROM stg_sales_clean
GROUP BY year_month
ORDER BY year_month

