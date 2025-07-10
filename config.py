# Kaggle dataset
DATASET = "larysa21/retail-data-american-football-gear-sales"

# Settings
ONLINE_FILE_NAME = "AF_online_sales_dataset.csv"
OFFLINE_FILE_NAME = "AF_offline_sales_dataset.csv"
RAW_DATA_FOLDER = "raw_data"
DOWNLOAD_TEMP = "tmp_download"
S3 = "s3"

# Schemas used in the project
SCHEMAS = ["prod", "playground"]

# MinIO Buckets
RAW_BUCKET = "raw-data"
UNPROCESSED_BUCKET = "unprocessed-files"
PROCESSED_BUCEKT = 'processed-files'

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
CREATE_TABLE_SCHEMAS = "db/ddl/create_schemas_tables.sql"

# Scripts to run in order
SCRIPTS = [
    "scripts/1_download_to_s3_raw.py",
    "scripts/2_create_schemas_tables.py",
    "scripts/3_load_dim_tables.py",
    "scripts/4_extract_raw_to_s3_daily.py"
]

