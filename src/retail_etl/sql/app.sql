-- Streamlit app queries. Load via utils.load_sql_section("app.sql", "<section>").

-- @section dataset_overview
SELECT
    COUNT(*) AS rows_count,
    COUNT(DISTINCT CustomerID) AS customers,
    COUNT(DISTINCT Country) AS countries,
    MIN(InvoiceDate) AS min_date,
    MAX(InvoiceDate) AS max_date
FROM stg_sales_clean
-- @end

-- @section staging_for_slicers
SELECT
    InvoiceNo,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    UnitPrice,
    CustomerID,
    Country,
    line_total
FROM stg_sales_clean
-- @end
