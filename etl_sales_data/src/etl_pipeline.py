from utils.spark import SparkSessionManager
from meta.config import DB_PATH, DB_tab_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, MapType
from pyspark.sql.functions import col, from_json
from pyspark.sql import DataFrame


class ETLPipeline:
    t_ord_reg_a_in = "order_region_a"
    t_ord_reg_b_in = "order_region_b"
    

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
        # Define the schema for the CSV files
        sales_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("OrderItemId", StringType(), True),
            StructField("QuantityOrdered", IntegerType(), True),
            StructField("ItemPrice", DoubleType(), True),
            StructField("PromotionDiscount", StringType(), True),  # As it's a JSON string
            StructField("batch_id", IntegerType(), True)
        ])
        sales_data_cols = ['OrderId', 'OrderItemId', 'QuantityOrdered', 'ItemPrice', 'PromotionDiscount', 'batch_id']
        # Load data from both regions
        df_a = self.spark.read_csv(file_name=self.t_ord_reg_a_in, schema=sales_schema)
        df_b = self.spark.read_csv(file_name=self.t_ord_reg_b_in, schema=sales_schema)

        # Define schema for PromotionDiscount
        promotion_schema = MapType(StringType(), StringType())
        def fetch_curr_n_dist(df : DataFrame):
            # Parse PromotionDiscount JSON string into a MapType
            df = df.select('*',from_json(col("PromotionDiscount"), promotion_schema).alias('PromotionDiscountmap'))\
                    .select("*", col('PromotionDiscountmap').getItem('CurrencyCode').alias('CurrencyCode')\
                            , col('PromotionDiscountmap').getItem('Amount').cast('double').alias('Amount'))
            
            df = df.selectExpr(['OrderId', 'OrderItemId', 'QuantityOrdered', 'ItemPrice', 'batch_id', 'CurrencyCode', 'Amount as PromotionDiscount'])
            return df
        
        # Add 'region' column to each dataset using select and selectExpr
        df_a = fetch_curr_n_dist(df_a).selectExpr('*', "'A' as region")
        df_b = fetch_curr_n_dist(df_b).selectExpr('*', "'B' as region")

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
        self.spark.write_to_db(final_df, DB_tab_name, DB_PATH)

