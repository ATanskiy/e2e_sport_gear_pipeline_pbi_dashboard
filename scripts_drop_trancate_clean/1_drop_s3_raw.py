import os
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

# Connect to MinIO
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

try:
    # List all objects in the bucket
    print(f"📂 Listing objects in '{BUCKET_NAME}'...")
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME)

    if 'Contents' in objects:
        print("🧹 Deleting files...")
        for obj in objects['Contents']:
            print(f"❌ Deleting {obj['Key']}...")
            s3.delete_object(Bucket=BUCKET_NAME, Key=obj['Key'])
    else:
        print("✅ Bucket is already empty.")

    # Delete the bucket
    print(f"💣 Deleting bucket '{BUCKET_NAME}'...")
    s3.delete_bucket(Bucket=BUCKET_NAME)
    print("✅ Bucket deleted successfully.")

except ClientError as e:
    print(f"❌ Error: {e}")
