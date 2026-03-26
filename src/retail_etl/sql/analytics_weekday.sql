SELECT
    CAST(strftime('%w', InvoiceDate) AS INTEGER) AS weekday_num,
    SUM(line_total) AS revenue,
    COUNT(DISTINCT InvoiceNo) AS invoices
FROM stg_sales_clean
GROUP BY weekday_num
ORDER BY weekday_num

