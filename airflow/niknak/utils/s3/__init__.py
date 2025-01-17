import boto3
import niknak.env as env
import smart_open
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union, Callable
from datetime import datetime, timezone
import logging

session = boto3.Session(
    profile_name=env.AWS_PROFILE_NAME,
    aws_access_key_id=env.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=env.AWS_SECRET_ACCESS_KEY,
)
s3 = session.client("s3", endpoint_url=env.S3_ENDPOINT_URL)
s3_resource = session.resource("s3")
transport_params = {"client": s3}


def split_path(file_path: str) -> dict:
    normalized_path = file_path.lstrip("/")

    bucket, path = normalized_path.split("/", 1)

    return {
        "Bucket": bucket,
        "Key": path,
    }


def read(path: Union[str, Path], mode: str = "r"):
    uri = f"s3://{str(path).lstrip('/')}" if not path.startswith("s3://") else path
    """Read a file from an s3 bucket
    """
    return smart_open.open(uri=uri, mode=mode, transport_params=transport_params)


def write(path: Union[str, Path], mode: str = "w"):
    uri = f"s3://{str(path).lstrip('/')}" if not path.startswith("s3://") else path
    """Write a file an s3 bucket
    """

    return smart_open.open(uri=uri, mode=mode, transport_params=transport_params)


def download(source_file: str, output_file: str):
    """Download a file from S3 to a local path."""
    source_info = split_path(source_file.replace("s3://", ""))
    bucket = source_info.get("Bucket")
    key = source_info.get("Key")

    logging.info(f"Downloading {source_file} to the bucket named {bucket} to the following output file: {output_file}")

    s3.download_file(bucket, key, output_file)


def upload(source_file: str, upload_path: str):
    path_info = split_path(upload_path.replace("s3://", ""))

    bucket = path_info.get("Bucket")
    key = path_info.get("Key")

    logging.info(f"Uploading {source_file} to the bucket named {bucket} with the following key: {key}")

    s3.upload_file(source_file, bucket, key)


def exists(path: str):
    try:
        s3.head_object(**split_path(path))
        return True
    except Exception as e:
        return False


def last_modified_at(path: str) -> Optional[datetime]:
    try:
        result = s3.head_object(**split_path(path))
        return result["LastModified"]
    except Exception as e:
        return None
