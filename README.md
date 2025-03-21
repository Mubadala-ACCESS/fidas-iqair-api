# IQAir Data Uploader: Detailed Documentation and Adaptation Guide

This document provides a comprehensive explanation of the Python script used to process and upload data to the IQAir API. The code is originally designed to read raw data from text files, compute hourly averages, store processing status in a SQLite database, write the aggregated results to CSV, and finally send the CSV data to an external API. This guide explains each component at a granular level and then outlines how to adapt the code to instead retrieve and process data directly from a SQL database (aggregated hourly averages).

---

## Table of Contents

1. [Overview](#overview)
2. [Code Structure and Functionality](#code-structure-and-functionality)
   - [Database Functions](#database-functions)
   - [File Listing Function](#file-listing-function)
   - [Processing and CSV Conversion](#processing-and-csv-conversion)
   - [API Call Function](#api-call-function)
   - [Main Loop](#main-loop)
3. [Adapting the Code for SQL Database Data](#adapting-the-code-for-sql-database-data)
4. [Testing and Debugging Considerations](#testing-and-debugging-considerations)
5. [Conclusion](#conclusion)
6. [References](#references)

---

## Overview

The original code automates the process of:
- **Reading raw sensor data** from `.txt` files.
- **Maintaining a processing state** in a SQLite database to avoid duplicate processing.
- **Aggregating the data on an hourly basis** using Pandas.
- **Saving the aggregated data** into CSV files.
- **Sending the CSV data** to an external API using a POST request.

This modular design allows easy tracking of which data rows have been processed and ensures only new data is handled during each iteration of the main loop.

---

## Code Structure and Functionality

### Database Functions

1. **`setup_database(db_path)`**  
   - **Purpose:**  
     Creates a table `processing_status` if it doesn’t exist. This table stores the file name, the last processed raw timestamp, the last processed aggregated (hourly) timestamp, and the last row number that was processed.
   - **Key Concepts:**  
     Uses SQLite to maintain state, ensuring idempotency in data processing.

2. **`get_processing_status_for_file(db_path, filename)`**  
   - **Purpose:**  
     Retrieves the processing status for a specific file. If no record is found, it returns default values.
   - **Detail:**  
     The function returns a tuple `(last_raw_timestamp, last_avg_timestamp, last_row)`, where `last_row` indicates how many lines were already processed.

3. **`update_processing_status(db_path, filename, last_raw_timestamp, last_avg_timestamp, last_row)`**  
   - **Purpose:**  
     Updates an existing record or inserts a new record into the `processing_status` table with the latest processing details.
   - **Mechanism:**  
     It conditionally constructs an SQL query based on the provided non-None parameters.

### File Listing Function

- **`get_relevant_txt_files(directory, db_path)`**  
  - **Purpose:**  
    Scans a specified directory for `.txt` files and filters them to include only those files that have new lines (i.e., the current line count exceeds the stored `last_row`).
  - **Key Detail:**  
    The function uses the stored processing state to avoid reprocessing data that has already been handled.

### Processing and CSV Conversion

- **`process_file(local_path, output_path, filename, output_filename, db_path)`**  
  - **Purpose:**  
    Processes a single text file by:
    1. Reading the file into a Pandas DataFrame.
    2. Identifying new rows (based on `last_row`).
    3. Parsing date and time columns to create a `date_time` column.
    4. Grouping data by hour (ensuring only complete hours are processed by comparing with the current hour).
    5. Calculating hourly averages for several sensor values (e.g., wind speed, temperature, particulate matter).
    6. Creating a new CSV DataFrame with a specific structure expected by the API.
    7. Appending or writing this CSV data to an output file.
    8. Updating the processing status in the SQLite database.
  - **Important Details:**  
    - **Date Parsing:** Uses `pd.to_datetime` with a specified format.
    - **Grouping:** Uses the DataFrame's `groupby` method to compute means for each hour.
    - **CSV Generation:** The resulting CSV includes hard-coded fields (e.g., station name, latitude, longitude) along with aggregated sensor values.
    
### API Call Function

- **`send_csv_data(dataframe, output_filename)`**  
  - **Purpose:**  
    Converts the CSV DataFrame into an in-memory CSV file and sends it to an API endpoint using a POST request.
  - **Mechanism:**  
    Utilizes the `requests` library to post the data, logs the response status and text, and returns the JSON response.
  - **Error Handling:**  
    Catches exceptions from the POST request and logs appropriate error messages.

### Main Loop

- **`main()` Function:**  
  - **Purpose:**  
    Serves as the orchestrator of the script, running in an infinite loop:
    1. Initializes the database.
    2. Determines directories for the raw text files and the output CSV files.
    3. Retrieves the list of relevant text files.
    4. Processes each file to compute hourly averages and update processing status.
    5. Sends the generated CSV data to the API.
    6. Waits for 60 seconds before repeating the process.
  - **Key Detail:**  
    The loop ensures that the system continuously checks for and processes new data, making it suitable for near-real-time data ingestion.

---

## Adapting the Code for SQL Database Data

To adapt this code to send the same aggregated data but derived directly from a SQL database rather than text files, consider the following steps:

### 1. Remove the File Reading Components

- **Eliminate Functions:**  
  Remove or bypass the `get_relevant_txt_files` and `process_file` functions that depend on reading and parsing `.txt` files.
- **Rationale:**  
  When the source data is already stored in a SQL database, the need to read external text files is obviated.

### 2. Create a New Function to Query the SQL Database

- **New Function:**  
  Define a function, e.g., `fetch_hourly_aggregates_from_db(db_connection_params)`, that:
  - Connects to your SQL database (could be PostgreSQL, MySQL, or even SQLite, depending on your setup).
  - Executes an SQL query that computes the hourly average of the sensor values.
  
- **Example SQL Query (SQLite syntax):**
  ```sql
  SELECT 
      strftime('%Y%m%dT%H%M', date_time) || "+0400" AS datetime,
      'Fidas Station (ACCESS)' AS name,
      24.5254 AS lat,
      54.4319 AS lon,
      AVG("wind speed") AS WV,
      AVG("wind direction") AS WD,
      AVG(T) AS TEMP,
      AVG(rH) AS HUMI,
      AVG(p) AS PRES,
      AVG(PM1) AS PM01,
      AVG("PM2.5") AS PM25,
      AVG(PM10) AS PM10
  FROM sensor_data
  WHERE date_time < datetime('now', 'localtime', '-1 hour')  -- only complete hours
  GROUP BY strftime('%Y%m%dT%H', date_time);
