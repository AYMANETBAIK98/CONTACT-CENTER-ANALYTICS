"""Create the 3 lakehouse buckets (bronze / silver / gold) in the S3 storage.

Run automatically by the "s3-init" service of docker compose.
Idempotent: a bucket that already exists is left as it is.
"""
from __future__ import annotations

import os
import sys
import time

import boto3
from botocore.config import Config

BUCKETS = ["bronze", "silver", "gold"]


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "lakehouse"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "lakehouse123"),
        region_name="us-east-1",
        config=Config(connect_timeout=3, retries={"max_attempts": 1}),
    )


def wait_for_s3(s3, attempts: int = 30) -> None:
    """The storage can take a few seconds to start: retry instead of failing at once."""
    for i in range(1, attempts + 1):
        try:
            s3.list_buckets()
            return
        except Exception as exc:
            print(f"S3 not ready yet ({i}/{attempts}): {type(exc).__name__}")
            time.sleep(2)
    raise SystemExit("S3 storage did not start in time")


def main() -> int:
    s3 = s3_client()
    wait_for_s3(s3)
    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    for bucket in BUCKETS:
        if bucket in existing:
            print(f"bucket {bucket}: already exists")
        else:
            s3.create_bucket(Bucket=bucket)
            print(f"bucket {bucket}: created")
    return 0


if __name__ == "__main__":
    sys.exit(main())
