import os
import boto3
from dotenv import load_dotenv
load_dotenv()

# Kaggle dataset
DATASET = "larysa21/retail-data-american-football-gear-sales"

# Settings
ONLINE_FILE_NAME = "AF_online_sales_dataset.csv"
OFFLINE_FILE_NAME = "AF_offline_sales_dataset.csv"
RAW_DATA_FOLDER = "raw_data"
DOWNLOAD_TEMP = "tmp_download"

# Load credentials from .env file
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_RAW = os.getenv("MINIO_RAW")
MINIO_UNPROCESSED=os.getenv("MINIO_UNPROCESSED")
MINIO_PROCESSED=os.getenv("MINIO_PROCESSED")

# List of unprocessed and processed buckets
BUCKET_LIST = [MINIO_UNPROCESSED, MINIO_PROCESSED]

# Initialize S3 client
S3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# Schemas used in the project
SCHEMAS = ["prod", "playground"]

# CSV File names
ONLINE_FILE_NAME = "AF_online_sales_dataset.csv"
OFFLINE_FILE_NAME = "AF_offline_sales_dataset.csv"

# Data folder paths
RAW_DATA_FOLDER = "raw_data"
SEEDS = "seeds"

# Seeds mapping
SEEDS_MAPPING = {
    "employees.csv": "employees",
    "payment_methods.csv": "payment_methods",
    "product_categories.csv": "product_categories",
    "product_subcategories.csv": "product_subcategories",
    "products.csv": "products",
    "shipping_methods.csv": "shipping_methods",
    "stores.csv": "stores"
}

# Sleep time between ETL batches (in seconds)
TIME_TO_SLEEP = 5

# Other shared settings
DATE_FORMAT = "%Y/%m/%d"

# Timestamp columns
TMSTMP =  "tmstmp"
DATE = "date"

# Paths
CREATE_TABLES_SCHEMAS = "db/ddl/create_schemas_tables.sql"
DROP_TABLE_SCHEMAS = "db/ddl/drop_schemas_tables.sql"
TRUNCATE_ALL_TABLES = "db/ddl/trancate_all_tables.sql"
TRUNCATE_DIM_TABLES = "db/ddl/trancate_dim_tables.sql"

# Scripts to run in order
SCRIPTS = [
    "scripts/1_download_to_s3_raw.py",
    "scripts/2_create_schemas_tables.py",
    "scripts/3_load_dim_tables.py",
    "scripts/4_extract_raw_to_s3_daily.py"
]

