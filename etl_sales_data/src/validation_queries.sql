-- Total record count
SELECT COUNT(*) AS total_records FROM sales_table;

-- Total sales amount by region
SELECT Region, SUM(ItemPrice * QuantityOrdered) AS total_sales
FROM sales_table
GROUP BY Region;

-- Average sales amount per transaction
SELECT AVG(ItemPrice * QuantityOrdered) AS avg_sales_per_transaction
FROM sales_table;

-- Check for duplicate OrderId
SELECT OrderId, COUNT(*) AS count
FROM sales_table
GROUP BY OrderId
HAVING count > 1;
