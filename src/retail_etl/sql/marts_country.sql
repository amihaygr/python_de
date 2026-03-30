SELECT
    Country,
    SUM(line_total) AS revenue,
    SUM(Quantity) AS units,
    COUNT(DISTINCT InvoiceNo) AS invoices,
    COUNT(DISTINCT CustomerID) AS customers
FROM stg_sales_clean
GROUP BY Country
ORDER BY revenue DESC

