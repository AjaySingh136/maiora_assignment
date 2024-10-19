**ETL Sales Data Pipeline & JokeAPI Integration**
=================================================

**Project Overview**
--------------------

This project is a Python-based **ETL pipeline** that extracts sales data from two different regions (Region A and Region B), transforms it as per the given business rules, and loads it into an **SQLite** database. The pipeline uses **PySpark** for data processing.

Additionally, an **API** is created using **Flask** to fetch jokes from the **JokeAPI**, process the data, and store it in a database.

### **Key Features:**

-   **ETL Pipeline:**
    -   Extracts data from two CSV files (`order_region_a.csv` and `order_region_b.csv`).
    -   Transforms the data based on specified business rules (calculating total sales, net sales, and filtering invalid records).
    -   Loads the cleaned data into an SQLite database.
    -   Validates the data with SQL queries (such as total sales by region, average sales per transaction, etc.).
-   **JokeAPI Integration:**
    -   Fetches 100 jokes from the **JokeAPI** (https://v2.jokeapi.dev).
    -   Stores the jokes in an SQLite database with relevant fields (category, type, joke content, etc.).

* * * * *

**Project Structure**
---------------------


```bash
etl_sales_data/
│
├── utils/
│   ├── spark.py                # Spark session and utility methods for data I/O
├── meta/
│   ├── config.py               # Database configuration
├── src/
│   ├── etl_pipeline.py         # Main ETL script
│   ├── db_queries.py           # SQL queries for validation
├── database/
│   ├── sales_data.db           # SQLite database file
│── api/
│   ├── app.py                  # Flask app for JokeAPI integration
├── test-data/
│   ├── order_region_a.csv      # Test data for region A
│   └── order_region_b.csv      # Test data for region B
├── tests/
│   ├── etl_pipeline_test.py    # Unit tests for ETL pipeline
│   └── api_test.py             # Unit tests for API
├── README.md                   # Documentation for running the project
└── requirements.txt            # Python dependencies
```
* * * * *

**Setup Instructions**
----------------------

Follow these steps to set up and run the project locally:

### **1\. Clone the Repository**

Clone the repository to your local machine:


`git clone <repo_url>
cd etl_sales_data`

### **2\. Install Dependencies**

Install the required Python dependencies and set python path and java path:

`pip install -r requirements.txt`
`export PYTHONPATH="/Users/ajaynegi/maiora_prj/maiora_assignment":"/Users/ajaynegi/maiora_prj/maiora_assignment/etl_sales_data":"/Users/ajaynegi/maiora_prj/maiora_assignment/etl_sales_data/src"`
`export JAVA_HOME="/opt/homebrew/opt/openjdk@11/"`

### **3\. Run the ETL Pipeline**

The ETL pipeline extracts data from the CSV files, transforms it as per the business rules, and loads it into the SQLite database.

To run the ETL pipeline:

`python src/etl_pipeline.py`

This will process the files `order_region_a.csv` and `order_region_b.csv` from the `test-data/` directory and store the processed data into `sales_data.db` located in the `database/` folder.

### **4\. Run Database Validation**

You can run SQL queries to validate the data loaded into the database:

`python src/db_queries.py`

This will show the following:

-   Total number of records.
-   Total sales by region.
-   Average sales per transaction.

### **5\. Run the Flask API**

The Flask API interacts with the JokeAPI to fetch and store jokes in the database.

To run the Flask API:

1.  Navigate to the `api/` directory:

`cd api`

2.  Start the Flask server:

`python app.py`

You can access the API at `http://127.0.0.1:5000`.

### **6\. Fetch Jokes from the JokeAPI**

To fetch jokes and store them in the database, visit the following endpoint:

http


`GET http://127.0.0.1:5000/fetch-jokes`

This will fetch 100 jokes from the JokeAPI and store them in the SQLite database (`sales_data.db`).

### **7\. Run Unit Tests**

You can test the ETL pipeline and the Flask API using the unit tests provided in the `tests/` folder.

To run the tests:



`python -m unittest discover -s tests`

* * * * *

**Assumptions**
---------------

-   **PySpark** is used to process the sales data due to its distributed data processing capabilities.
-   **SQLite** is used as the database for simplicity.
-   The JokeAPI provides jokes with either "single" or "twopart" types, and we process both.
-   The `sales_data.db` SQLite database is created automatically during the ETL process.

* * * * *

**API Endpoints**
-----------------

-   `/fetch-jokes`: Fetches 100 jokes from **JokeAPI** and stores them in the SQLite database.

* * * * *

**Additional Information**
--------------------------

-   The **ETL Pipeline** script reads two CSV files (`order_region_a.csv` and `order_region_b.csv`) from the `test-data/` folder. These files contain sales data with columns like `OrderId`, `OrderItemId`, `QuantityOrdered`, `ItemPrice`, and `PromotionDiscount`.

-   The **Flask API** calls the JokeAPI to fetch jokes, stores them in the SQLite database, and handles operations via REST endpoints.

* * * * *

**Requirements**
----------------

-   Python 3.x
-   PySpark
-   Flask
-   SQLite (comes with Python)
-   `requests` library (for API interaction)

Install Python dependencies using:



`pip install -r requirements.txt`

* * * * *

**License**
-----------

This project is open-source and available under the MIT License. Feel free to modify or use it as you wish.

* * * * *

### **Conclusion**

This project provides a complete solution for processing sales data using PySpark, loading it into a database, and building a Flask-based API that integrates with JokeAPI. The provided steps should allow you to set up the project and run both the ETL pipeline and the API locally.