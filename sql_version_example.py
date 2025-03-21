#!/usr/bin/env python3
import os
import io
import time
import datetime
import sqlite3
import requests
import pandas as pd
from appConfig import DB_PATH, CSV_PATH, API_URL, HEADERS

# A constant key for tracking sensor data processing
PROCESSING_KEY = "sensor_data"

#############################################
# Processing Status Functions
#############################################
def setup_processing_db(db_path):
    """
    Ensures that the processing_status table exists to track the latest processed sensor data.
    This table uses a constant key (e.g., 'sensor_data') for our sensor data source.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processing_status (
            key TEXT PRIMARY KEY,
            last_raw_timestamp TEXT,
            last_avg_timestamp TEXT
        )
    ''')
    conn.commit()
    conn.close()

def get_processing_status(db_path, key):
    """
    Retrieves the processing status for the given key.
    Returns a tuple: (last_raw_timestamp, last_avg_timestamp).
    If no record exists, returns (None, None).
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT last_raw_timestamp, last_avg_timestamp FROM processing_status WHERE key = ?', (key,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return result[0] if result[0] != "" else None, result[1] if result[1] != "" else None
    else:
        return None, None

def update_processing_status(db_path, key, last_raw_timestamp=None, last_avg_timestamp=None):
    """
    Updates or inserts the processing status for the given key.
    Only provided (non-None) fields are updated.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT key FROM processing_status WHERE key = ?', (key,))
    result = cursor.fetchone()
    if result:
        update_fields = []
        params = []
        if last_raw_timestamp is not None:
            update_fields.append("last_raw_timestamp = ?")
            params.append(last_raw_timestamp)
        if last_avg_timestamp is not None:
            update_fields.append("last_avg_timestamp = ?")
            params.append(last_avg_timestamp)
        if update_fields:
            query = "UPDATE processing_status SET " + ", ".join(update_fields) + " WHERE key = ?"
            params.append(key)
            cursor.execute(query, tuple(params))
    else:
        cursor.execute('INSERT INTO processing_status (key, last_raw_timestamp, last_avg_timestamp) VALUES (?, ?, ?)',
                       (key, last_raw_timestamp if last_raw_timestamp is not None else "",
                        last_avg_timestamp if last_avg_timestamp is not None else ""))
    conn.commit()
    conn.close()

#############################################
# Sensor Data Retrieval and Processing
#############################################
def fetch_new_sensor_data(db_path):
    """
    Fetches sensor data from the 'sensor_data' table in the database.
    Only rows with a combined datetime greater than the last processed raw timestamp are returned.
    Assumes the table has columns: date, time, wind speed, wind direction, T, rH, p, PM1, PM2.5, PM10.
    """
    conn = sqlite3.connect(db_path)
    query = "SELECT * FROM sensor_data"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        return pd.DataFrame()
    
    # Combine 'date' and 'time' columns into a datetime column.
    try:
        df['date_time'] = pd.to_datetime(df['date'] + ' ' + df['time'],
                                         format='%m/%d/%Y %I:%M:%S %p')
    except Exception as e:
        print(f"{datetime.datetime.now()}: Error parsing date and time from sensor_data: {e}")
        return pd.DataFrame()
    
    # Retrieve the last processed raw timestamp from processing status.
    last_raw_timestamp, _ = get_processing_status(db_path, PROCESSING_KEY)
    if last_raw_timestamp:
        try:
            # Stored timestamp format is 'YYYYmmddTHHMM+0400' so we take the datetime part.
            last_dt = datetime.datetime.strptime(last_raw_timestamp[:13], "%Y%m%dT%H%M")
        except Exception as e:
            print(f"{datetime.datetime.now()}: Error parsing last_raw_timestamp: {e}")
            last_dt = None
    else:
        last_dt = None
    
    # Filter rows to include only new sensor data.
    if last_dt:
        df_new = df[df['date_time'] > last_dt].copy()
    else:
        df_new = df.copy()
    
    return df_new

def process_sensor_data(df_new):
    """
    Processes new sensor data to compute hourly averages for completed hours.
    Returns a tuple (csv_data, new_last_raw, new_last_avg) where:
      - csv_data: DataFrame ready for CSV export,
      - new_last_raw: the maximum raw timestamp processed,
      - new_last_avg: the timestamp corresponding to the latest aggregated hour.
    """
    if df_new.empty:
        print(f"{datetime.datetime.now()}: No new sensor data available.")
        return None, None, None
    
    # Floor date_time to the nearest hour.
    df_new['hour'] = df_new['date_time'].dt.floor('h')
    current_hour = pd.Timestamp.now().floor('h')
    
    # Only process complete hours (exclude the current hour).
    complete_data = df_new[df_new['hour'] < current_hour]
    if complete_data.empty:
        print(f"{datetime.datetime.now()}: No new complete hour data to process.")
        return None, None, None
    
    try:
        # Group by the 'hour' column and compute the mean of each sensor measurement.
        group = complete_data.groupby('hour').agg({
            'wind speed': 'mean',
            'wind direction': 'mean',
            'T': 'mean',
            'rH': 'mean',
            'p': 'mean',
            'PM1': 'mean',
            'PM2.5': 'mean',
            'PM10': 'mean'
        }).reset_index()
    except Exception as e:
        print(f"{datetime.datetime.now()}: Error during grouping/averaging: {e}")
        return None, None, None

    # Format the aggregated hour into a string and append the timezone offset.
    group['datetime'] = group['hour'].dt.strftime('%Y%m%dT%H%M') + "+0400"
    
    # Build the final CSV DataFrame with additional station details.
    csv_data = pd.DataFrame({
        'datetime': group['datetime'],
        'name': 'Fidas Station (ACCESS)',
        'lat': 24.5254,
        'lon': 54.4319,
        'WV': group['wind speed'],
        'WD': group['wind direction'],
        'TEMP': group['T'],
        'HUMI': group['rH'],
        'PRES': group['p'],
        'PM01': group['PM1'],
        'PM25': group['PM2.5'],
        'PM10': group['PM10']
    })
    
    # Determine new timestamps:
    # - new_last_avg: the most recent hourly aggregated timestamp.
    # - new_last_raw: the maximum raw timestamp processed from the complete data.
    new_last_avg = group['datetime'].iloc[-1]
    new_last_raw = complete_data['date_time'].max().floor('min').strftime('%Y%m%dT%H%M') + "+0400"
    
    return csv_data, new_last_raw, new_last_avg

#############################################
# API Call Function
#############################################
def send_csv_data(dataframe, output_filename):
    """
    Converts the DataFrame to an in-memory CSV and sends it to the IQAir API.
    Logs the API response and returns the JSON response.
    """
    buffer = io.StringIO()
    dataframe.to_csv(buffer, index=False)
    buffer.seek(0)
    files = {'file': (output_filename, buffer, 'text/csv')}
    try:
        response = requests.post(API_URL, headers=HEADERS, files=files)
        print(f"{datetime.datetime.now()}: API response status: {response.status_code}")
        print(f"{datetime.datetime.now()}: API response text: {response.text}")
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"{datetime.datetime.now()}: Error sending CSV to API: {e}")
        return None

#############################################
# Main Loop
#############################################
def main():
    # Ensure the processing status table exists.
    setup_processing_db(DB_PATH)
    
    while True:
        # Fetch new sensor data from the SQL database.
        df_new = fetch_new_sensor_data(DB_PATH)
        if df_new.empty:
            print(f"{datetime.datetime.now()}: No new sensor data found.")
        else:
            # Process the data by grouping and averaging by hour.
            csv_data, new_last_raw, new_last_avg = process_sensor_data(df_new)
            if csv_data is not None:
                # Construct a unique CSV filename using the current datetime.
                formatted_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"NYUAD_FIDAS_DATA_{formatted_datetime}.csv"
                output_file = os.path.join(CSV_PATH, output_filename)
                try:
                    csv_data.to_csv(output_file, index=False)
                    print(f"{datetime.datetime.now()}: CSV file saved as {output_file}")
                except Exception as e:
                    print(f"{datetime.datetime.now()}: Error writing CSV file: {e}")
                
                # Update the processing status with the latest timestamps.
                update_processing_status(DB_PATH, PROCESSING_KEY,
                                         last_raw_timestamp=new_last_raw,
                                         last_avg_timestamp=new_last_avg)
                
                # Send the CSV data to the API.
                api_response = send_csv_data(csv_data, output_filename)
                print(f"{datetime.datetime.now()}: CSV sent to API. Response: {api_response}")
            else:
                print(f"{datetime.datetime.now()}: No new complete hourly data to process.")
        
        # Sleep for 60 seconds before checking again.
        next_run = datetime.datetime.now() + datetime.timedelta(seconds=60)
        sleep_seconds = (next_run - datetime.datetime.now()).total_seconds()
        print(f"{datetime.datetime.now()}: Sleeping for {int(sleep_seconds)} seconds until next check at {next_run}.")
        time.sleep(sleep_seconds)

if __name__ == "__main__":
    main()
