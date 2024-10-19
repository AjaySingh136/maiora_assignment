import unittest
from pyspark.sql import SparkSession
from etl_sales_data.src.etl_pipeline import ETLPipeline
from etl_sales_data.utils.spark import SparkSessionManager
from etl_sales_data.meta import config
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
class TestETLPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Initialize SparkSessionManager and any other configuration needed for the test.
        """
        # Spark configuration setup
        cls.config = {
            'path_out': "test-files/sales/out/",
            'path_in': "test-files/sales_in/",
            'db_sales_path_in': "etl_sales_data/databases/",
            'partitions_size': '4',
            'spark_executors': '2',
            'spark_driver_memory': '2g'
        }
        # Wrap the SparkSession in SparkSessionManager
        cls.spark_manager = SparkSessionManager(cls.config)
        cls.etl_pipeline = ETLPipeline(cls.spark_manager)

    def validate(self, df: DataFrame):
        # 1. Count the total number of records
        total_records = df.count()
        print(f"Total number of records: {total_records}")

        # 2. Find the total sales amount by region
        # Assuming there's a "Region" column and sales amount is calculated as QuantityOrdered * ItemPrice
        if 'Region' in df.columns and 'QuantityOrdered' in df.columns and 'ItemPrice' in df.columns:
            total_sales_by_region = df.withColumn('SalesAmount', F.col('QuantityOrdered') * F.col('ItemPrice')) \
                                    .groupBy("Region") \
                                    .agg(F.sum('SalesAmount').alias('TotalSales'))
            print("Total sales amount by region:")
            total_sales_by_region.show()
        else:
            print("Required columns (Region, QuantityOrdered, ItemPrice) for total sales calculation are missing.")

        # 3. Find the average sales amount per transaction
        if 'QuantityOrdered' in df.columns and 'ItemPrice' in df.columns:
            avg_sales_per_transaction = df.withColumn('SalesAmount', F.col('QuantityOrdered') * F.col('ItemPrice')) \
                                        .agg(F.mean('SalesAmount').alias('AvgSalesPerTransaction')).collect()[0]['AvgSalesPerTransaction']
            print(f"Average sales amount per transaction: {avg_sales_per_transaction}")
        else:
            print("Required columns (QuantityOrdered, ItemPrice) for average sales calculation are missing.")

        # 4. Ensure there are no duplicate OrderId values
        if 'OrderId' in df.columns:
            duplicate_orders = df.groupBy("OrderId").count().filter(F.col("count") > 1)
            num_duplicates = duplicate_orders.count()

            if num_duplicates > 0:
                print(f"Found {num_duplicates} duplicate OrderId(s):")
                duplicate_orders.show()
            else:
                print("No duplicate OrderId values found.")
        else:
            print("OrderId column is missing.")



    def test_etl_pipeline(self):
        """
        Test if the ETL pipeline runs successfully without throwing errors.
        """
 
        try:
            # Run the ETL process
            self.etl_pipeline.process_sales_data()
            final_df = self.spark_manager.read_from_db(table_name=config.DB_tab_name, db_path=config.DB_PATH)
            # Validate the DataFrame is not empty
            self.assertGreater(final_df.count(), 0, "The final DataFrame should not be empty")
            self.validate(df=final_df)
        except Exception as e:
            print(f"ETL pipeline failed with error: {e}")
            self.assertTrue(False)

if __name__ == '__main__':
    unittest.main()
