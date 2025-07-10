import boto3
import os
from dotenv import load_dotenv

load_dotenv()

# Connect to MinIO
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

BUCKETS = ["unprocessed-data", "processed-data"]

def delete_all_objects(bucket):
    response = s3.list_objects_v2(Bucket=bucket)
    objects = [{'Key': obj['Key']} for obj in response.get('Contents', [])]

    if not objects:
        print(f"🧺 Bucket '{bucket}' is already empty.")
        return

    s3.delete_objects(Bucket=bucket, Delete={'Objects': objects})
    print(f"🗑️ Deleted {len(objects)} objects from bucket '{bucket}'.")

if __name__ == "__main__":
    for bucket in BUCKETS:
        delete_all_objects(bucket)