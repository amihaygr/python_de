SELECT
    StockCode,
    Description,
    SUM(line_total) AS revenue,
    SUM(Quantity) AS units,
    COUNT(DISTINCT InvoiceNo) AS invoices
FROM stg_sales_clean
GROUP BY StockCode, Description
ORDER BY revenue DESC

