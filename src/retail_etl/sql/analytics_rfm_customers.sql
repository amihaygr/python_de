SELECT
    CustomerID,
    MAX(InvoiceDate) AS last_invoice,
    COUNT(DISTINCT InvoiceNo) AS frequency,
    SUM(line_total) AS monetary
FROM stg_sales_clean
GROUP BY CustomerID

