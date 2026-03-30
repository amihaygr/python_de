-- Mart rebuild queries (aggregates from staging). Load via utils.load_sql_section("marts.sql", "<section>").

-- @section monthly
SELECT
    strftime('%Y-%m', InvoiceDate) AS year_month,
    SUM(line_total) AS revenue,
    SUM(Quantity) AS units,
    COUNT(DISTINCT InvoiceNo) AS invoices
FROM stg_sales_clean
GROUP BY year_month
ORDER BY year_month
-- @end

-- @section product
SELECT
    StockCode,
    Description,
    SUM(line_total) AS revenue,
    SUM(Quantity) AS units,
    COUNT(DISTINCT InvoiceNo) AS invoices
FROM stg_sales_clean
GROUP BY StockCode, Description
ORDER BY revenue DESC
-- @end

-- @section country
SELECT
    Country,
    SUM(line_total) AS revenue,
    SUM(Quantity) AS units,
    COUNT(DISTINCT InvoiceNo) AS invoices,
    COUNT(DISTINCT CustomerID) AS customers
FROM stg_sales_clean
GROUP BY Country
ORDER BY revenue DESC
-- @end

-- @section customer
SELECT
    CustomerID,
    SUM(line_total) AS revenue,
    SUM(Quantity) AS units,
    COUNT(DISTINCT InvoiceNo) AS invoices
FROM stg_sales_clean
GROUP BY CustomerID
ORDER BY revenue DESC
-- @end
