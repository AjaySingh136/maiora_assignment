# src/db_queries.py

import sqlite3
from etl_sales_data.meta.config import DB_PATH

def validate_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Total number of records
    cursor.execute("SELECT COUNT(*) FROM sales_data")
    print(f"Total Records: {cursor.fetchone()[0]}")

    # Total sales by region
    cursor.execute("SELECT region, SUM(total_sales) FROM sales_data GROUP BY region")
    print(f"Total Sales by Region: {cursor.fetchall()}")

    # Average sales per transaction
    cursor.execute("SELECT AVG(net_sales) FROM sales_data")
    print(f"Average Sales per Transaction: {cursor.fetchone()[0]}")

    conn.close()

if __name__ == "__main__":
    validate_data()
