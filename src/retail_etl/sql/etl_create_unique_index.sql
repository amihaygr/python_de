CREATE UNIQUE INDEX IF NOT EXISTS ux_sales_key
ON stg_sales_clean (InvoiceNo, StockCode, CustomerID, InvoiceDate)

