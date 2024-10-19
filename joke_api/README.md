# Joke API

This is a Flask-based API that fetches jokes from an external API, stores them in a SQLite database, and allows users to retrieve stored jokes. It includes Swagger documentation for easy access to the API endpoints.

## Table of Contents
- [Features](#features)
- [Requirements](#requirements)
- [Setup](#setup)
- [Running the Application](#running-the-application)
- [API Endpoints](#api-endpoints)
- [Swagger Documentation](#swagger-documentation)
- [License](#license)

## Features
- Fetches jokes from the JokeAPI.
- Stores jokes in a SQLite database.
- Provides endpoints to view and fetch jokes.
- Swagger UI for API documentation.

## Requirements
- Python 3.x
- Flask
- Flask-SQLAlchemy
- Requests
- Flasgger

## Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd <repository-directory>


1.  **Create a virtual environment (optional but recommended)**

    bash

    

    `python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate``

2.  **Install the required packages**

    bash

    

    `pip install Flask Flask-SQLAlchemy requests flasgger`

3.  **Configure Database Path** Make sure you have a valid path for your SQLite database. Modify the `etl_sales_data/meta/config.py` file to define the `Jokes_DB_PATH` variable if it is not already set.

Running the Application
-----------------------

1.  Run the Flask application:

    bash

    

    `python app.py`

2.  The application will start on `http://127.0.0.1:5000`.

API Endpoints
-------------

-   **GET /**\
    Returns a welcome message and usage information.

-   **POST /fetch_jokes**\
    Fetches jokes from the external API and stores them in the database.

    -   **Response**:
        -   `201` - Jokes fetched and stored successfully.
-   **GET /jokes**\
    Retrieves all stored jokes from the database.

    -   **Response**:
        -   `200` - A list of jokes in JSON format.

Swagger Documentation
---------------------

Once the application is running, you can access the Swagger UI documentation at:

arduino



`http://127.0.0.1:5000/apidocs`

License
-------

This project is licensed under the MIT License - see the LICENSE file for details.

vbnet



 `### Additional Notes:
1. **Replace `<repository-url>` and `<repository-directory>`** with the actual URL of your repository and the directory name where the app is located.
2. Ensure that your `etl_sales_data/meta/config.py` file is set up correctly to provide the `Jokes_DB_PATH`.
3. You can adjust the content of the README to better fit your project's needs or structure.`

4o mini