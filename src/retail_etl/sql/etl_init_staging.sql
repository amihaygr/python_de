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

