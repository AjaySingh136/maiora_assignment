from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from etl_sales_data.meta import config

class SparkSessionManager:
    def __init__(self, config, schema: StructType = None):
        self.config = config  # Store configuration
        self.spark = SparkSession.builder \
            .appName("SalesETL") \
            .config("spark.executor.instances", config.get('spark_executors', '1')) \
            .config("spark.driver.memory", config.get('spark_driver_memory', '1g')) \
            .config("spark.jars", "etl_sales_data/jars/sqlite-jdbc-3.46.1.3.jar") \
            .config("spark.sql.shuffle.partitions", config.get('partitions_size', '4')) \
            .getOrCreate()
        # .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.1") \
        # Define the input and output paths as attributes of the class
        self.path_in = config.get('path_in', "test-data/sales/in/")
        self.path_out = config.get('path_out', "test-data/sales/out/")
        self.db_sales_path_in = config.get('db_sales_path_in', "etl_sales_data/databases/")

    def get_spark_session(self):
        return self.spark

    def read_csv(self, file_name: str = "order_region_a.csv", copy_from_path: str = None, schema : StructType = None) -> DataFrame:
        """
        Read a CSV file from the provided path or the default input path with a given schema.
        
        :param file_name: The name of the CSV file to read (default is "order_region_a.csv").
        :param copy_from_path: The path to read the file from. Defaults to self.path_in.
        :return: A DataFrame containing the CSV data.
        """
        # Use the default 'path_in' if no path is provided
        path_in = copy_from_path if copy_from_path else self.path_in
        file_path = f"{path_in}/{file_name}.csv"

        # Read the CSV file using the provided schema or inferSchema if no schema is provided
        if schema:
            return self.spark.read.csv(file_path, header=True, schema=schema)
        else:
            return self.spark.read.csv(file_path, header=True, inferSchema=True)



    def read_excel(self, file_name: str = "order_region_a.xlsx", copy_from_path: str = None, schema: StructType = None) -> DataFrame:
        """
        Read an Excel file from the provided path or the default input path with a given schema.
        
        :param file_name: The name of the Excel file to read (default is "order_region_a.xlsx").
        :param copy_from_path: The path to read the file from. Defaults to self.path_in.
        :return: A DataFrame containing the Excel data.
        """
        # Use the default 'path_in' if no path is provided
        path_in = copy_from_path if copy_from_path else self.path_in
        file_path = f"{path_in}/{file_name}"

        # Read the Excel file using the provided schema
        if schema:
            return self.spark.read \
                .format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("escape", "\"") \
                .option("nullValue", "null") \
                .load(file_path)
        else:
            return self.spark.read \
                .format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("escape", "\"") \
                .option("nullValue", "null") \
                .load(file_path)
            
    def write_to_db(self, df: DataFrame, table_name: str, db_path: str = None):
        """
        Write a DataFrame to the SQLite database.
        
        :param df: The DataFrame to write.
        :param table_name: The table name in the database.
        :param db_path: The path to the database file. Defaults to self.db_sales_path_in.
        """
        # Use the default 'db_sales_path_in' if no db path is provided
        db_sales_path_in = db_path if db_path else self.db_sales_path_in
        df.write.format("jdbc").options(
            url=f"jdbc:sqlite:{db_sales_path_in}",
            dbtable=table_name,
            driver="org.sqlite.JDBC"
        ).mode('overwrite').save()

    def write_csv(self, df: DataFrame, file_name: str = "output.csv", write_to_path: str = None, mode_to_write: str = "overwrite"):
        """
        Write a DataFrame to a CSV file.

        :param df: The DataFrame to write.
        :param file_name: The name of the output CSV file (default is "output.csv").
        :param write_to_path: The path to write the CSV file to. Defaults to self.path_out.
        :param mode_to_write: The mode for writing the file (default is "overwrite").
        """
        # Use the default 'path_out' if no path is provided
        path_out = write_to_path if write_to_path else self.path_out
        file_path = f"{path_out}/{file_name}"

        # Write the DataFrame to CSV
        df.write.csv(file_path, header=True, mode=mode_to_write)


    def read_from_db(self, table_name: str, db_path: str = None) -> DataFrame:
        """
        Read data from an SQLite database into a Spark DataFrame.
        
        :param table_name: The table name in the SQLite database.
        :param db_path: The path to the database file. Defaults to self.db_sales_path_in.
        :return: A Spark DataFrame containing the table data.
        """
        # Use the default 'db_sales_path_in' if no db path is provided
        db_sales_path_in = db_path if db_path else self.db_sales_path_in
        jdbc_url = f"jdbc:sqlite:{db_sales_path_in}"

        # Load the table from the SQLite database into a DataFrame
        df = self.spark.read.format("jdbc").options(
            url=jdbc_url,
            dbtable=table_name,
            driver="org.sqlite.JDBC"
        ).load()

        return df