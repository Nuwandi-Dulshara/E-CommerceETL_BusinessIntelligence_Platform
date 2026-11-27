-- Gold Parquet imports are performed with bound paths by warehouse.py.
CREATE VIEW transactions AS
SELECT f.*, d.date, p.stock_code, p.description, c.customer_id,
       f.gross_amount - f.return_amount AS net_amount
FROM fact_transactions f
JOIN dim_date d USING (date_key)
JOIN dim_product p USING (product_key)
JOIN dim_customer c USING (customer_key);
