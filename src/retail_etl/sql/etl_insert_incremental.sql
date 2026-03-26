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

