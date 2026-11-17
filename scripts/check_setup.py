"""Step 0 check: are the 3 services up and ready?

    docker compose run --rm tools python scripts/check_setup.py

Checks that:
  1. PostgreSQL is reachable and has the 4 schemas
  2. Redpanda (Kafka) is reachable and has the 2 topics
  3. RustFS (S3) is reachable and has the 3 lakehouse buckets
"""
from __future__ import annotations

import os
import sys

import boto3
import psycopg2
from botocore.config import Config
from confluent_kafka.admin import AdminClient

EXPECTED_SCHEMAS = {"realtime", "staging", "marts", "monitoring"}
EXPECTED_TOPICS = {"contact-events", "agent-events"}
EXPECTED_BUCKETS = {"bronze", "silver", "gold"}


def check_postgres() -> bool:
    dsn = (
        f"host={os.getenv('WAREHOUSE_HOST', 'localhost')} "
        f"port={os.getenv('WAREHOUSE_PORT', '5435')} "
        f"dbname={os.getenv('WAREHOUSE_DB', 'contact_center')} "
        f"user={os.getenv('WAREHOUSE_USER', 'cc')} "
        f"password={os.getenv('WAREHOUSE_PASSWORD', 'cc')}"
    )
    try:
        with psycopg2.connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
            cur.execute("SELECT split_part(version(), ',', 1)")
            print(f"[OK]   PostgreSQL reachable: {cur.fetchone()[0]}")
            cur.execute("SELECT schema_name FROM information_schema.schemata")
            missing = EXPECTED_SCHEMAS - {r[0] for r in cur.fetchall()}
    except psycopg2.OperationalError as exc:
        print(f"[FAIL] PostgreSQL not reachable: {exc}".strip())
        return False
    if missing:
        print(f"[FAIL] PostgreSQL schemas missing: {sorted(missing)}")
        return False
    print(f"[OK]   PostgreSQL schemas: {sorted(EXPECTED_SCHEMAS)}")
    return True


def check_redpanda() -> bool:
    brokers = os.getenv("KAFKA_BROKERS", "localhost:19092")
    try:
        metadata = AdminClient({"bootstrap.servers": brokers}).list_topics(timeout=10)
    except Exception as exc:  # the client raises KafkaException when the broker is down
        print(f"[FAIL] Redpanda not reachable on {brokers}: {exc}")
        return False
    print(f"[OK]   Redpanda reachable: {len(metadata.brokers)} broker(s) on {brokers}")
    topics = {name: len(t.partitions) for name, t in metadata.topics.items() if not name.startswith("_")}
    missing = EXPECTED_TOPICS - topics.keys()
    if missing:
        print(f"[FAIL] Kafka topics missing: {sorted(missing)} (did redpanda-init run?)")
        return False
    print("[OK]   Kafka topics: " + ", ".join(f"{t} ({topics[t]} partitions)" for t in sorted(EXPECTED_TOPICS)))
    return True


def check_s3() -> bool:
    endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "lakehouse"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "lakehouse123"),
        region_name="us-east-1",
        config=Config(connect_timeout=5, retries={"max_attempts": 1}),
    )
    try:
        buckets = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    except Exception as exc:
        print(f"[FAIL] S3 storage (RustFS) not reachable on {endpoint}: {exc}")
        return False
    print(f"[OK]   S3 storage (RustFS) reachable on {endpoint}")
    missing = EXPECTED_BUCKETS - buckets
    if missing:
        print(f"[FAIL] S3 buckets missing: {sorted(missing)} (did s3-init run?)")
        return False
    print(f"[OK]   S3 buckets: {sorted(EXPECTED_BUCKETS)}")
    return True


def main() -> int:
    results = [check_postgres(), check_redpanda(), check_s3()]
    ok = all(results)
    print("\nEnvironment ready. On to step 1!" if ok else "\nSomething is missing, see [FAIL] lines above.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
