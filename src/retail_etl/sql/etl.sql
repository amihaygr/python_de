-- Staging + incremental ETL. Load via utils.load_sql_section("etl.sql", "<section>").

-- @section init_staging
CREATE TABLE IF NOT EXISTS stg_sales_clean (
    InvoiceNo TEXT,
    StockCode TEXT,
    Description TEXT,
    Quantity INTEGER,
    InvoiceDate TEXT,
    UnitPrice REAL,
    CustomerID INTEGER,
    Country TEXT,
    line_total REAL
)
-- @end

-- @section create_unique_index
CREATE UNIQUE INDEX IF NOT EXISTS ux_sales_key
ON stg_sales_clean (InvoiceNo, StockCode, CustomerID, InvoiceDate)
-- @end

-- @section drop_staging
DROP TABLE IF EXISTS stg_sales_clean;
DROP INDEX IF EXISTS ux_sales_key;
-- @end

-- @section insert_incremental
INSERT OR IGNORE INTO stg_sales_clean (
    InvoiceNo,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    UnitPrice,
    CustomerID,
    Country,
    line_total
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
-- @end

-- @section select_max_invoice_date
SELECT MAX(InvoiceDate) AS max_date
FROM stg_sales_clean
-- @end
