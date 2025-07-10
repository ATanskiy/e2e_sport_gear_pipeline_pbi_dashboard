import os
import sys 
import time
import boto3
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from io import StringIO
from botocore.exceptions import ClientError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import ONLINE_FILE_NAME, OFFLINE_FILE_NAME, S3, TIME_TO_SLEEP,\
                     TMSTMP, DATE, DATE_FORMAT, UNPROCESSED_BUCKET

load_dotenv()

# MinIO config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
RAW_BUCKET = os.getenv("MINIO_BUCKET_NAME")  # Same bucket you created

# Connect to MinIO
s3 = boto3.client(
    S3,
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# Scans all objects in unprocessed-data, parses year/month/day from object keys, returns
# a sorted list of all days that have already been processed
def list_existing_dates():
    result = s3.list_objects_v2(Bucket=UNPROCESSED_BUCKET)
    existing_dates = set()

    for obj in result.get("Contents", []):
        key = obj["Key"]
        parts = key.split("/")
        if len(parts) >= 3:
            year, month, day = parts[:3]
            existing_dates.add(f"{year}/{month}/{day}")

    return sorted(existing_dates)

# starts from earliest date in raw data, skips all already in unprocessed-data, returns next date
def get_next_date(start, processed_dates):
    date = start
    while date.strftime(DATE_FORMAT) in processed_dates:
        date += timedelta(days=1)
    return date

# reads csv from s3, parses date column, sorts by date, returns clean df
def read_csv_from_s3(bucket, key, date_column):
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'])
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    df = df.sort_values(by=date_column)
    return df

# converts df to csv in memory, uploads to s3 under the path, confirms with a print
def write_csv_to_s3(df, bucket, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print(f"Saved {key}")

# filters both dfs (online, offline) to get only records from the given date, 
# creates a filder-like prefix YYYY/MM/DD/, uploads one online.csv and one offline.csv
def process_one_day(raw_online, raw_offline, current_date):
    # Filter data for the current date
    online_day = raw_online[raw_online[TMSTMP].dt.date == current_date.date()]
    offline_day = raw_offline[raw_offline[DATE].dt.date == current_date.date()]

    year, month, day = current_date.strftime(DATE_FORMAT).split("/")
    prefix = f"{year}/{month}/{day}/"

    write_csv_to_s3(online_day, UNPROCESSED_BUCKET, f"{prefix}online.csv")
    write_csv_to_s3(offline_day, UNPROCESSED_BUCKET, f"{prefix}offline.csv")

# loads full df into memory once, finds earliest date from both sources,
# in a loop: gets processed dates from s3, finds next date to process, saves per day CSVs
# to unprocessed-data bucket, sleeps for selected time
def main_loop():
    # Load raw files once into memory
    raw_online = read_csv_from_s3(RAW_BUCKET, ONLINE_FILE_NAME, TMSTMP)
    raw_offline = read_csv_from_s3(RAW_BUCKET, OFFLINE_FILE_NAME, DATE)

    # Get date range from data
    min_date = min(raw_online[TMSTMP].min(), raw_offline[DATE].min())
    min_date = min_date.replace(hour=0, minute=0, second=0, microsecond=0)

    while True:
        print("Checking what has already been processed...")
        existing = list_existing_dates()

        next_date = get_next_date(min_date, existing)
        print(f'Processing day: {next_date.strftime(DATE_FORMAT)}')

        process_one_day(raw_online, raw_offline, next_date)

        print(f"Sleeping for {TIME_TO_SLEEP} seconds...\n")
        time.sleep(TIME_TO_SLEEP)

if __name__ == "__main__":
    ## Ensure processed-data bucket exists
    try:
        s3.head_bucket(Bucket=UNPROCESSED_BUCKET)
    except ClientError:
        print(f"Creating bucket '{UNPROCESSED_BUCKET}'...")
        s3.create_bucket(Bucket=UNPROCESSED_BUCKET)

    main_loop()