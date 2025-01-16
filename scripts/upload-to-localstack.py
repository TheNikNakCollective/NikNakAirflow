import boto3
import argparse
from pathlib import Path

LOCALSTACK_ENDPOINT = "http://localhost:4566"
AWS_ACCESS_KEY_ID = "test"
AWS_SECRET_ACCESS_KEY = "test"

s3 = boto3.client(
    "s3",
    endpoint_url=LOCALSTACK_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


def upload_file(file_path: str, bucket_name: str, s3_key: str):
    """Upload a file to LocalStack S3."""

    s3.upload_file(file_path, bucket_name, s3_key)
    
    print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload files to LocalStack S3.")
    parser.add_argument("file", type=str, help="Path to the file to upload")
    parser.add_argument("bucket", type=str, help="S3 bucket name")
    parser.add_argument("key", type=str, help="S3 key (path in the bucket)")

    args = parser.parse_args()

    # Call the upload function
    upload_file(args.file, args.bucket, args.key)
