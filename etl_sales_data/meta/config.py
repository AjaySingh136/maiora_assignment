# meta/config.py

DB_PATH = "etl_sales_data/databases/sales_data.db"
DB_tab_name = "sales_data"

# Test run for main config

job_config = {
    'path_out': "test-files/sales/out/",
    'path_in': "test-files/sales_in/",
    'db_sales_path_in': "etl_sales_data/databases/",
    'partitions_size': '4',
    'spark_executors': '2',
    'spark_driver_memory': '2g'
}