import sys
from pyspark.sql import SparkSession
from src.etl_pipeline import ETLPipeline  # Adjust the import if the structure changes
from utils.spark import SparkSessionManager  # Adjust the import as needed
from meta.config import job_config  # Import your config settings if needed

def main():
    # Initialize Spark session manager and ETL pipeline
    
    spark_manager = SparkSessionManager(job_config)
    etl_pipeline = ETLPipeline(spark_manager)
    
    # Run the sales data processing
    etl_pipeline.process_sales_data()

if __name__ == "__main__":
    main()
