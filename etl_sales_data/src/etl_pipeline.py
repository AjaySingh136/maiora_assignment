from utils.spark import SparkSessionManager
from meta.config import DB_PATH

class ETLPipeline:
    def __init__(self, spark: SparkSessionManager):
        """
        Initialize the ETL pipeline with a SparkSessionManager.
        
        :param spark: Instance of SparkSessionManager
        """
        self.spark = spark

    def process_sales_data(self):
        """
        The main ETL process that:
        - Reads sales data from CSV files.
        - Performs transformations like adding 'region', calculating 'total_sales', and filtering rows.
        - Writes the transformed data to the database.
        
        :return: Final transformed DataFrame
        """
        # Load data from both regions
        df_a = self.spark.read_csv(file_name='order_region_a.csv')
        df_b = self.spark.read_csv(file_name='order_region_b.csv')

        # Add 'region' column to each dataset using select and selectExpr
        df_a = df_a.selectExpr('*', "'A' as region")
        df_b = df_b.selectExpr('*', "'B' as region")

        # Combine both datasets
        combined_df = df_a.union(df_b)

        # Add total_sales and net_sales columns using selectExpr
        combined_df = combined_df.selectExpr(
            "*",
            "QuantityOrdered * ItemPrice as total_sales",
            "QuantityOrdered * ItemPrice - PromotionDiscount as net_sales"
        )

        # Filter rows with positive net_sales
        filtered_df = combined_df.filter("net_sales > 0")

        # Remove duplicates based on OrderId
        final_df = filtered_df.dropDuplicates(["OrderId"])

        # Write the transformed data to the SQLite database
        self.spark.write_to_db(final_df, 'sales_data', DB_PATH)

        return final_df
