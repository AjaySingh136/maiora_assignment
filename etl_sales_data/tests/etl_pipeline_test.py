import unittest
from pyspark.sql import SparkSession
from src.etl_pipeline import ETLPipeline
from utils.spark import SparkSessionManager

class TestETLPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Initialize SparkSessionManager and any other configuration needed for the test.
        """
        # Spark configuration setup
        cls.config = {
            'path_out': "test-data/sales/out/",
            'path_in': "test-data/sales/in/",
            'db_sales_path_in': "test-data/sales_inp/",
            'partitions_size': '4',
            'spark_executors': '2',
            'spark_driver_memory': '2g'
        }

        # Create a Spark session with the above configuration
        spark_session = SparkSession.builder \
            .appName("ETL Test") \
            .config("spark.executor.instances", cls.config.get('spark_executors', '2')) \
            .config("spark.driver.memory", cls.config.get('spark_driver_memory', '2g')) \
            .config("spark.sql.shuffle.partitions", cls.config.get('partitions_size', '4')) \
            .getOrCreate()

        # Wrap the SparkSession in SparkSessionManager
        cls.spark_manager = SparkSessionManager(cls.config)
        cls.etl_pipeline = ETLPipeline(cls.spark_manager)

    def test_etl_pipeline(self):
        """
        Test if the ETL pipeline runs successfully without throwing errors.
        """
        try:
            # Run the ETL process
            final_df = self.etl_pipeline.process_sales_data()

            # Validate the DataFrame is not empty
            self.assertGreater(final_df.count(), 0, "The final DataFrame should not be empty")
        except Exception as e:
            print(f"ETL pipeline failed with error: {e}")
            self.assertTrue(False)

if __name__ == '__main__':
    unittest.main()
