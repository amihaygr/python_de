SELECT
    CustomerID,
    SUM(line_total) AS revenue,
    SUM(Quantity) AS units,
    COUNT(DISTINCT InvoiceNo) AS invoices
FROM stg_sales_clean
GROUP BY CustomerID
ORDER BY revenue DESC

