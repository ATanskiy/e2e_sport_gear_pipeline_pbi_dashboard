# Schemas used in the project
SCHEMAS = ["prod", "playground"]

# MinIO Buckets
RAW_BUCKET = "raw-data"
UNPROCESSED_BUCKET = "unprocessed-data"

# CSV File names
ONLINE_FILE_NAME = "AF_online_sales_dataset.csv"
OFFLINE_FILE_NAME = "AF_offline_sales_dataset.csv"

# Data folder paths
RAW_DATA_FOLDER = "raw_data"
SEEDS_FOLDER = "seeds"

# Sleep time between ETL batches (in seconds)
TIME_TO_SLEEP = 5

# Other shared settings
DATE_FORMAT = "%Y-%m-%d"