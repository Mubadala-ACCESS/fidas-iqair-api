#!/usr/bin/env python3
"""FIDAS text via FTP → CSV → IQAir uploader.

This script:
- Connects to an FTP server where FIDAS .txt files are stored.
- Tracks per-file processing state in a SQLite database.
- Aggregates raw 1-minute (or similar) data to hourly means for
  completed hours.
- Appends/creates monthly CSVs in a local directory.
- Sends each new batch of hourly rows to the IQAir OpenAir API
  as a CSV upload.
"""

from __future__ import annotations

import datetime
import io
import logging
import os
import sqlite3
import time
from ftplib import FTP, all_errors as FTP_ERRORS
from typing import List, Optional, Tuple

import pandas as pd
import requests

from appConfig import (
    API_URL,
    CSV_PATH,
    DB_PATH,
    FTP_HOST,
    FTP_HOME_DIR,
    FTP_PASSWORD,
    FTP_PORT,
    FTP_USERNAME,
    HEADERS,
)

LOGGER = logging.getLogger("iqair_uploader")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def setup_database(db_path: str) -> None:
    """Create the processing_status table if it does not exist.

    The table stores, per source file:

    - filename: raw FIDAS file name (primary key).
    - last_raw_timestamp: latest raw timestamp processed (string).
    - last_avg_timestamp: latest aggregated hourly timestamp sent (string).
    - last_row: number of raw lines already processed.
    """
    connection = sqlite3.connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS processing_status (
                filename TEXT PRIMARY KEY,
                last_raw_timestamp TEXT,
                last_avg_timestamp TEXT,
                last_row INTEGER DEFAULT 0
            )
            """
        )
        connection.commit()
    finally:
        connection.close()


def get_processing_status_for_file(
    db_path: str,
    filename: str,
) -> Tuple[Optional[str], Optional[str], int]:
    """Return (last_raw_timestamp, last_avg_timestamp, last_row) for a file.

    If there is no entry for *filename*, (None, None, 0) is returned.
    """
    connection = sqlite3.connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT last_raw_timestamp, last_avg_timestamp, last_row "
            "FROM processing_status WHERE filename = ?",
            (filename,),
        )
        result = cursor.fetchone()
    finally:
        connection.close()

    if result is None:
        return None, None, 0

    last_raw_timestamp, last_avg_timestamp, last_row = result
    return last_raw_timestamp, last_avg_timestamp, last_row or 0


def update_processing_status(
    db_path: str,
    filename: str,
    last_raw_timestamp: Optional[str] = None,
    last_avg_timestamp: Optional[str] = None,
    last_row: Optional[int] = None,
) -> None:
    """Insert or update processing_status row for *filename*.

    Only non-None fields are updated on existing rows. For new rows,
    missing values are stored as empty string or 0.
    """
    connection = sqlite3.connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT filename FROM processing_status WHERE filename = ?",
            (filename,),
        )
        exists = cursor.fetchone() is not None

        if exists:
            update_fields: List[str] = []
            params: List[object] = []

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
                query = (
                    "UPDATE processing_status SET "
                    + ", ".join(update_fields)
                    + " WHERE filename = ?"
                )
                params.append(filename)
                cursor.execute(query, tuple(params))
        else:
            cursor.execute(
                """
                INSERT INTO processing_status (
                    filename,
                    last_raw_timestamp,
                    last_avg_timestamp,
                    last_row
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    filename,
                    last_raw_timestamp or "",
                    last_avg_timestamp or "",
                    last_row or 0,
                ),
            )

        connection.commit()
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# FTP helpers
# ---------------------------------------------------------------------------


def create_ftp_client() -> Optional[FTP]:
    """Create, connect, and authenticate a plain FTP client.

    NOTE:
        The remote server does not support AUTH TLS (FTPS). This client
        therefore uses unencrypted FTP. Ensure this is acceptable in
        your environment (e.g., local/VPN-only traffic).
    """
    try:
        ftp = FTP()
        ftp.connect(host=FTP_HOST, port=FTP_PORT, timeout=30)
        ftp.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)
        LOGGER.info(
            "Connected to FTP server %s:%s as user '%s'.",
            FTP_HOST,
            FTP_PORT,
            FTP_USERNAME,
        )

        # Attempt to change into the configured home directory. If the
        # server chroots the user, this may fail; in that case we stay
        # in the default directory and continue.
        if FTP_HOME_DIR:
            try:
                ftp.cwd(FTP_HOME_DIR)
                LOGGER.info(
                    "Changed FTP working directory to '%s'.", FTP_HOME_DIR
                )
            except FTP_ERRORS as exc:
                LOGGER.warning(
                    "Could not change to FTP_HOME_DIR '%s': %s. "
                    "Continuing in current directory.",
                    FTP_HOME_DIR,
                    exc,
                )

        return ftp
    except FTP_ERRORS as exc:
        LOGGER.error("FTP connection or login failed: %s", exc)
        return None


def list_remote_txt_files(ftp: FTP) -> List[str]:
    """List all ``.txt`` files in the current FTP directory.

    Args:
        ftp: Active FTP client.

    Returns:
        Sorted list of ``.txt`` filenames.
    """
    try:
        names = ftp.nlst()
    except FTP_ERRORS as exc:
        LOGGER.error("Failed to list FTP directory: %s", exc)
        return []

    txt_files = sorted(
        name for name in names if name.lower().endswith(".txt")
    )
    return txt_files


def read_remote_file_to_dataframe(
    ftp: FTP,
    filename: str,
) -> Optional[pd.DataFrame]:
    """Retrieve a remote text file over FTP and load it into a DataFrame.

    The file is assumed to be a tab-separated text file with headers.
    """
    buffer = io.BytesIO()

    try:
        ftp.retrbinary(f"RETR {filename}", callback=buffer.write)
    except FTP_ERRORS as exc:
        LOGGER.error("Error retrieving '%s' over FTP: %s", filename, exc)
        return None

    buffer.seek(0)
    try:
        dataframe = pd.read_table(buffer)
    except (pd.errors.EmptyDataError, OSError, UnicodeDecodeError) as exc:
        LOGGER.error("Error parsing '%s' into DataFrame: %s", filename, exc)
        return None
    finally:
        buffer.close()

    return dataframe


# ---------------------------------------------------------------------------
# Processing and CSV conversion
# ---------------------------------------------------------------------------


def build_output_filename(filename: str, now: datetime.datetime) -> str:
    """Construct the monthly CSV filename for a given raw file name.

    Raw files are expected to follow:
        ``DUSTMONITOR_17712_<year>_<month>.txt``

    If this pattern cannot be parsed, a timestamp-based fallback name
    is used.
    """
    base_name = os.path.basename(filename)
    parts = base_name.split("_")

    if len(parts) >= 4:
        year = parts[2]
        month_and_ext = parts[3]
        month, *_ = month_and_ext.split(".")
        if year.isdigit() and month.isdigit():
            return f"NYUAD_FIDAS_DATA_{year}_{month}.csv"

    formatted_datetime = now.strftime("%Y%m%d_%H%M%S")
    return f"NYUAD_FIDAS_DATA_{formatted_datetime}.csv"


def process_file(
    ftp: FTP,
    output_path: str,
    filename: str,
    output_filename: str,
    db_path: str,
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Process a single remote FIDAS file and append hourly means to CSV.

    The function:

    - Retrieves *filename* from the FTP server.
    - Uses the processing_status table to determine how many rows were
      already processed for this file.
    - Computes hourly mean values for completed hours only.
    - Appends/creates the corresponding monthly CSV file under
      *output_path*.
    - Updates processing_status for the file.

    Returns:
        (dataframe, csv_filename) where *dataframe* contains only the
        newly processed hourly rows. If there is nothing new to send,
        both are None.
    """
    (
        last_raw_timestamp,
        last_avg_timestamp,
        last_row,
    ) = get_processing_status_for_file(db_path, filename)

    LOGGER.debug(
        "Processing '%s' (last_raw=%s, last_avg=%s, last_row=%d).",
        filename,
        last_raw_timestamp,
        last_avg_timestamp,
        last_row,
    )

    dataframe = read_remote_file_to_dataframe(ftp, filename)
    if dataframe is None:
        # Error already logged in read_remote_file_to_dataframe.
        return None, None

    total_rows = len(dataframe)
    if total_rows == 0:
        LOGGER.info("File '%s' is empty.", filename)
        update_processing_status(db_path, filename, last_row=0)
        return None, None

    if last_row > total_rows:
        LOGGER.warning(
            "Stored last_row (%d) is greater than total_rows (%d) "
            "for '%s'. Resetting last_row to 0.",
            last_row,
            total_rows,
            filename,
        )
        last_row = 0

    if total_rows == last_row:
        LOGGER.info("No new raw data in '%s'.", filename)
        return None, None

    df_new = dataframe.iloc[last_row:].copy()

    try:
        df_new.loc[:, "date_time"] = pd.to_datetime(
            df_new.loc[:, "date"] + " " + df_new.loc[:, "time"],
            format="%m/%d/%Y %I:%M:%S %p",
        )
    except KeyError as exc:
        LOGGER.error(
            "Required 'date' or 'time' column missing in '%s': %s",
            filename,
            exc,
        )
        return None, None
    except ValueError as exc:
        LOGGER.error("Error parsing date/time in '%s': %s", filename, exc)
        return None, None

    df_new.loc[:, "hour"] = df_new.loc[:, "date_time"].dt.floor("h")
    current_hour = pd.Timestamp.now().floor("h")

    complete_data = df_new[df_new.loc[:, "hour"] < current_hour]
    if complete_data.empty:
        LOGGER.info("No new completed hours to process in '%s'.", filename)
        update_processing_status(db_path, filename, last_row=total_rows)
        return None, None

    try:
        grouped = complete_data.groupby("hour").agg(
            {
                "wind speed": "mean",
                "wind direction": "mean",
                "T": "mean",
                "rH": "mean",
                "p": "mean",
                "PM1": "mean",
                "PM2.5": "mean",
                "PM10": "mean",
            }
        )
    except KeyError as exc:
        LOGGER.error(
            "Missing expected column while aggregating '%s': %s", filename, exc
        )
        return None, None

    grouped = grouped.reset_index()
    grouped.loc[:, "datetime"] = (
        grouped.loc[:, "hour"].dt.strftime("%Y%m%dT%H%M") + "+0400"
    )

    csv_data = pd.DataFrame(
        {
            "datetime": grouped.loc[:, "datetime"],
            "name": "Fidas Station (ACCESS)",
            "lat": 24.5254,
            "lon": 54.4319,
            "WV": grouped.loc[:, "wind speed"],
            "WD": grouped.loc[:, "wind direction"],
            "TEMP": grouped.loc[:, "T"],
            "HUMI": grouped.loc[:, "rH"],
            "PRES": grouped.loc[:, "p"],
            "PM01": grouped.loc[:, "PM1"],
            "PM25": grouped.loc[:, "PM2.5"],
            "PM10": grouped.loc[:, "PM10"],
        }
    )

    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, output_filename)

    try:
        if os.path.exists(output_file):
            csv_data.to_csv(
                output_file,
                mode="a",
                header=False,
                index=False,
            )
        else:
            csv_data.to_csv(output_file, index=False)
    except OSError as exc:
        LOGGER.error("Error writing CSV '%s': %s", output_file, exc)
        return None, None

    new_last_avg = grouped.loc[:, "datetime"].iloc[-1]
    new_last_raw = (
        complete_data.loc[:, "date_time"]
        .max()
        .floor("min")
        .strftime("%Y%m%dT%H%M")
        + "+0400"
    )

    update_processing_status(
        db_path,
        filename,
        last_raw_timestamp=new_last_raw,
        last_avg_timestamp=new_last_avg,
        last_row=total_rows,
    )

    LOGGER.info(
        "Updated '%s' with hourly aggregated data from '%s'.",
        output_filename,
        filename,
    )
    return csv_data, output_filename


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------


def send_csv_data(
    dataframe: pd.DataFrame,
    output_filename: str,
) -> Optional[dict]:
    """Send a CSV DataFrame to the IQAir OpenAir API.

    Args:
        dataframe: DataFrame containing hourly-aggregated rows.
        output_filename: Name used for the uploaded CSV file.

    Returns:
        Parsed JSON response from the API on success, or ``None`` on failure.
    """
    buffer = io.StringIO()
    try:
        dataframe.to_csv(buffer, index=False)
        buffer.seek(0)

        files = {
            "file": (output_filename, buffer, "text/csv"),
        }

        try:
            response = requests.post(
                API_URL,
                headers=HEADERS,
                files=files,
                timeout=30,
            )
        except requests.RequestException as exc:
            LOGGER.error("Network error sending CSV to API: %s", exc)
            return None

        LOGGER.info(
            "API response status for '%s': %s",
            output_filename,
            response.status_code,
        )
        LOGGER.debug("API response text: %s", response.text)

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            LOGGER.error(
                "API returned error for '%s': %s", output_filename, exc
            )
            return None

        try:
            return response.json()
        except ValueError:
            LOGGER.error(
                "Failed to decode JSON response for '%s'. Raw text: %s",
                output_filename,
                response.text,
            )
            return None
    finally:
        buffer.close()


# ---------------------------------------------------------------------------
# Main loop helpers
# ---------------------------------------------------------------------------


def sleep_until_next_run(interval_seconds: int = 60) -> None:
    """Sleep until the next scheduled run.

    The next run is simply ``now + interval_seconds``. This keeps the
    loop readable and close to the original behaviour (check roughly
    every minute).
    """
    now = datetime.datetime.now()
    next_run = now + datetime.timedelta(seconds=interval_seconds)
    sleep_seconds = max(0, int((next_run - now).total_seconds()))

    LOGGER.info(
        "Sleeping for %d seconds until next check at %s.",
        sleep_seconds,
        next_run.isoformat(timespec="seconds"),
    )
    time.sleep(sleep_seconds)


def main() -> None:
    """Entry point for the FIDAS → IQAir upload loop."""
    setup_database(DB_PATH)

    while True:
        ftp = create_ftp_client()
        if ftp is None:
            LOGGER.error(
                "FTP client could not be created; skipping this cycle and "
                "will retry after the sleep interval.",
            )
        else:
            try:
                txt_files = list_remote_txt_files(ftp)
                if not txt_files:
                    LOGGER.info(
                        "No .txt files found in FTP directory '%s'.",
                        FTP_HOME_DIR or "<current>",
                    )

                for filename in txt_files:
                    now = datetime.datetime.now()
                    output_filename = build_output_filename(filename, now)

                    csv_data, final_output_filename = process_file(
                        ftp=ftp,
                        output_path=CSV_PATH,
                        filename=filename,
                        output_filename=output_filename,
                        db_path=DB_PATH,
                    )

                    if csv_data is not None:
                        api_response = send_csv_data(
                            dataframe=csv_data,
                            output_filename=(
                                final_output_filename or output_filename
                            ),
                        )
                        LOGGER.info(
                            "CSV sent to IQAir API for '%s'. API response: %s",
                            filename,
                            api_response,
                        )
                    else:
                        LOGGER.info(
                            "No new hourly data to send from '%s'.",
                            filename,
                        )
            finally:
                try:
                    ftp.quit()
                except FTP_ERRORS:
                    # Some servers may close the connection early; ignore.
                    try:
                        ftp.close()
                    except FTP_ERRORS:
                        pass

        # ~every minute; keep aligned with your previous behaviour.
        sleep_until_next_run(interval_seconds=60)


if __name__ == "__main__":
    main()
