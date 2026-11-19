"""BRONZE: copy the landing files into the S3 lakehouse, exactly as they are.

    docker compose run --rm tools python scripts/ingest_bronze.py

data/landing/contacts/date=2026-11-04/market=UK/contacts.jsonl
  -> s3://bronze/contacts/date=2026-11-04/market=UK/contacts.jsonl

Bronze = raw and untouched: if a later step has a bug, the original data is still there to rebuild from.
Idempotent: a file already in bronze with the same content (same MD5) is skipped.
"""
from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent.parent
LANDING = Path(os.getenv("LANDING_DIR", ROOT / "data" / "landing"))   # LANDING_DIR: Airflow writes elsewhere
BUCKET = "bronze"
# one-part uploads up to 100 MB, so the ETag stays the MD5 of the file (needed by already_there)
SINGLE_PART = TransferConfig(multipart_threshold=100 * 1024 * 1024)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "lakehouse"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "lakehouse123"),
        region_name="us-east-1",
        config=Config(retries={"max_attempts": 3}),
    )


def md5(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def already_there(s3, key: str, checksum: str) -> bool:
    """For a simple upload, the S3 ETag is the MD5 of the content."""
    try:
        return s3.head_object(Bucket=BUCKET, Key=key)["ETag"].strip('"') == checksum
    except ClientError:
        return False


def main() -> int:
    files = sorted(p for p in LANDING.rglob("*") if p.is_file() and p.name != ".gitkeep")
    if not files:
        print(f"No file in {LANDING}. Run the generator first (step 1).")
        return 1
    s3 = s3_client()
    uploaded = skipped = 0
    for path in files:
        key = path.relative_to(LANDING).as_posix()          # contacts/date=.../market=.../contacts.jsonl
        checksum = md5(path)
        if already_there(s3, key, checksum):
            skipped += 1
            continue
        s3.upload_file(str(path), BUCKET, key, Config=SINGLE_PART)
        uploaded += 1
    print(f"Bronze: {uploaded} file(s) uploaded, {skipped} already there (same content), "
          f"{len(files)} in total -> s3://{BUCKET}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
