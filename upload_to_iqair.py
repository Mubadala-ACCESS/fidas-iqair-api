#!/usr/bin/env python3
import os
import io
import time
import datetime
import sqlite3
import requests
import pandas as pd
from appConfig import DB_PATH, TXT_PATH, CSV_PATH, API_URL, HEADERS

#############################################
# Database Functions
#############################################
def setup_database(db_path):
    """
    Create a table to store processing status for each file including:
      - filename: the file being processed (primary key),
      - last_raw_timestamp: the most recent raw (unaggregated) timestamp processed,
      - last_avg_timestamp: the most recent aggregated (hourly) timestamp sent,
      - last_row: the number of rows already processed from the file.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processing_status (
            filename TEXT PRIMARY KEY,
            last_raw_timestamp TEXT,
            last_avg_timestamp TEXT,
            last_row INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()

def get_processing_status_for_file(db_path, filename):
    """
    Retrieve processing status for a given file.
    Returns a tuple: (last_raw_timestamp, last_avg_timestamp, last_row)
    or (None, None, 0) if no record exists.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT last_raw_timestamp, last_avg_timestamp, last_row FROM processing_status WHERE filename = ?', (filename,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return result[0], result[1], result[2] if result[2] is not None else 0
    else:
        return None, None, 0

def update_processing_status(db_path, filename, last_raw_timestamp=None, last_avg_timestamp=None, last_row=None):
    """
    Update or insert the processing status for the given filename.
    Only provided fields (non-None) are updated.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT filename FROM processing_status WHERE filename = ?', (filename,))
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
        if last_row is not None:
            update_fields.append("last_row = ?")
            params.append(last_row)
        if update_fields:
            query = "UPDATE processing_status SET " + ", ".join(update_fields) + " WHERE filename = ?"
            params.append(filename)
            cursor.execute(query, tuple(params))
    else:
        cursor.execute('INSERT INTO processing_status (filename, last_raw_timestamp, last_avg_timestamp, last_row) VALUES (?, ?, ?, ?)',
                       (filename, last_raw_timestamp if last_raw_timestamp is not None else "",
                        last_avg_timestamp if last_avg_timestamp is not None else "",
                        last_row if last_row is not None else 0))
    conn.commit()
    conn.close()

#############################################
# File Listing Function
#############################################
def get_relevant_txt_files(directory, db_path):
    """
    List all .txt files in the directory and return those that have new lines
    (i.e., current line count > stored last_row for that file).
    """
    txt_files = [f for f in os.listdir(directory) if f.lower().endswith('.txt')]
    txt_files.sort()
    relevant_files = []
    for f in txt_files:
        file_path = os.path.join(directory, f)
        try:
            with open(file_path, 'r') as fp:
                line_count = sum(1 for line in fp)
        except Exception as e:
            print(f"{datetime.datetime.now()}: Error counting lines in {f}: {e}")
            continue
        _, _, stored_last_row = get_processing_status_for_file(db_path, f)
        if line_count > stored_last_row:
            relevant_files.append(f)
    return relevant_files

#############################################
# Processing and CSV Conversion
#############################################
def process_file(local_path, output_path, filename, output_filename, db_path):
    """
    Read a text file, compute hourly averages (only for completed hours),
    append the averages to a CSV file, and update the processing status with:
      - last_raw_timestamp: most recent raw timestamp processed,
      - last_avg_timestamp: most recent aggregated timestamp sent,
      - last_row: total rows processed.
    Processes only new lines based on the stored last_row.
    """
    last_raw_timestamp, last_avg_timestamp, last_row = get_processing_status_for_file(db_path, filename)
    file_path = os.path.join(local_path, filename)
    try:
        df = pd.read_table(file_path)
    except Exception as e:
        print(f"{datetime.datetime.now()}: Error reading {filename}: {e}")
        return None, None

    total_rows = len(df)
    # Process only new lines if file has been processed before
    df_new = df.iloc[last_row:].copy() if total_rows > last_row else pd.DataFrame()
    
    if df_new.empty:
        print(f"{datetime.datetime.now()}: No new raw data in {filename}.")
        return None, None

    try:
        df_new.loc[:, 'date_time'] = pd.to_datetime(
            df_new.loc[:, 'date'] + ' ' + df_new.loc[:, 'time'],
            format='%m/%d/%Y %I:%M:%S %p'
        )
    except Exception as e:
        print(f"{datetime.datetime.now()}: Error parsing date and time in {filename}: {e}")
        return None, None

    df_new.loc[:, 'hour'] = df_new.loc[:, 'date_time'].dt.floor('h')
    current_hour = pd.Timestamp.now().floor('h')

    complete_data = df_new[df_new.loc[:, 'hour'] < current_hour]
    if complete_data.empty:
        print(f"{datetime.datetime.now()}: No new complete hour data to process in {filename}.")
        update_processing_status(db_path, filename, last_row=total_rows)
        return None, None

    try:
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
        print(f"{datetime.datetime.now()}: Error during grouping/averaging in {filename}: {e}")
        return None, None

    group['datetime'] = group['hour'].dt.strftime('%Y%m%dT%H%M') + "+0400"

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

    output_file = os.path.join(output_path, output_filename)
    try:
        if os.path.exists(output_file):
            csv_data.to_csv(output_file, mode='a', header=False, index=False)
        else:
            csv_data.to_csv(output_file, index=False)
    except Exception as e:
        print(f"{datetime.datetime.now()}: Error writing CSV for {filename}: {e}")
        return None, None

    new_last_avg = group['datetime'].iloc[-1]
    new_last_raw = complete_data['date_time'].max().floor('min').strftime('%Y%m%dT%H%M') + "+0400"
    update_processing_status(db_path, filename, last_raw_timestamp=new_last_raw, last_avg_timestamp=new_last_avg, last_row=total_rows)
    print(f"{datetime.datetime.now()}: Updated {output_filename} with hourly aggregated data from {filename}")
    return csv_data, output_filename

#############################################
# API Call Function
#############################################
def send_csv_data(dataframe, output_filename):
    """
    Sends the CSV DataFrame to the API by converting it to an in-memory CSV.
    Logs the API response status and text.
    Returns the JSON response from the API.
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
    db_path = DB_PATH
    setup_database(db_path)
    txt_file_path = TXT_PATH
    csv_file_path = CSV_PATH

    while True:
        txt_files = get_relevant_txt_files(txt_file_path, db_path)
        if not txt_files:
            print(f"{datetime.datetime.now()}: No txt files with new data found in {txt_file_path}.")
        else:
            for filename in txt_files:
                now = datetime.datetime.now()
                # Extract year and month from the filename (expected format: DUSTMONITOR_17712_<year>_<month>.txt)
                try:
                    parts = filename.split('_')
                    if len(parts) >= 4:
                        year = parts[2]
                        month = parts[3].split('.')[0]
                        output_filename = f"NYUAD_FIDAS_DATA_{year}_{month}.csv"
                    else:
                        raise ValueError("Filename does not have expected parts")
                except Exception:
                    formatted_datetime = now.strftime("%Y%m%d_%H%M%S")
                    output_filename = f"NYUAD_FIDAS_DATA_{formatted_datetime}.csv"
                
                csv_data, unique_output_filename = process_file(txt_file_path, csv_file_path, filename, output_filename, db_path)
                if csv_data is not None:
                    api_response = send_csv_data(csv_data, unique_output_filename)
                    print(f"{datetime.datetime.now()}: CSV sent to API. Response: {api_response}")
                else:
                    print(f"{datetime.datetime.now()}: No new hourly data to send from {filename}.")

        # Check for new data every minute
        now = datetime.datetime.now()
        next_run = now + datetime.timedelta(seconds=60)
        sleep_seconds = (next_run - now).total_seconds()
        print(f"{datetime.datetime.now()}: Sleeping for {int(sleep_seconds)} seconds until next check at {next_run}.")
        time.sleep(sleep_seconds)

if __name__ == "__main__":
    main()
