import boto3
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")

response = s3.list_objects_v2(
    Bucket=bucket,
    Prefix="customers/"
)

total_records = 0
all_ids = []

for obj in response.get("Contents", []):
    key = obj["Key"]

    if not key.endswith(".parquet"):
        continue

    filename = os.path.basename(key)

    s3.download_file(bucket, key, filename)

    df = pd.read_parquet(filename)

    print(f"{key} → {len(df)} records")

    total_records += len(df)
    all_ids.extend(df["id"].tolist())

    os.remove(filename)

print("\n========================")
print(f"TOTAL RECORDS: {total_records}")
print(f"UNIQUE IDs: {len(set(all_ids))}")
print(f"MIN ID: {min(all_ids)}")
print(f"MAX ID: {max(all_ids)}")
print("========================")