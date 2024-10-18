from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

class SparkSessionManager:
    def __init__(self, config):
        self.config = config  # Store configuration
        self.spark = SparkSession.builder \
            .appName("SalesETL") \
            .config("spark.executor.instances", config.get('spark_executors', '1')) \
            .config("spark.driver.memory", config.get('spark_driver_memory', '1g')) \
            .config("spark.sql.shuffle.partitions", config.get('partitions_size', '4')) \
            .getOrCreate()

        # Define the input and output paths as attributes of the class
        self.path_in = config.get('path_in', "test-data/sales/in/")
        self.path_out = config.get('path_out', "test-data/sales/out/")
        self.db_sales_path_in = config.get('db_sales_path_in', "test-data/sales_inp/")

    def get_spark_session(self):
        return self.spark

    def read_csv(self, file_name: str = "order_region_a.csv", mode_to_write: str = "overwrite", copy_from_path: str = None) -> DataFrame:
        """
        Read a CSV file from the provided path or the default input path.
        
        :param file_name: The name of the CSV file to read (default is "order_region_a.csv").
        :param mode_to_write: Mode in which to read/write data (default is "overwrite").
        :param copy_from_path: The path to read the file from. Defaults to self.path_in.
        :return: A DataFrame containing the CSV data.
        """
        # Use the default 'path_in' if no path is provided
        path_in = copy_from_path if copy_from_path else self.path_in
        file_path = f"{path_in}/{file_name}"

        # Read the CSV file and return DataFrame
        return self.spark.read.csv(file_path, header=True, inferSchema=True)

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
