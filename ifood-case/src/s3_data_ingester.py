import os
import logging
import tempfile
from pathlib import Path

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

BUCKET     = "tlc-export-lake"
BASE_URL   = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTHS     = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05"]
TAXI_TYPES = ["yellow", "green"]

logging.basicConfig(format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S", level=logging.INFO)
log = logging.getLogger(__name__)


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def already_uploaded(s3, key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


def download_file(url, dest):
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=65536):
            f.write(chunk)


def run():
    s3 = get_s3_client()

    with tempfile.TemporaryDirectory() as tmp:
        for taxi_type in TAXI_TYPES:
            for month in MONTHS:
                file_name  = f"{taxi_type}_tripdata_{month}.parquet"
                s3_key     = f"landing_zone/{taxi_type}_taxi/{file_name}"
                local_path = Path(tmp) / file_name

                log.info("%s", file_name)

                if already_uploaded(s3, s3_key):
                    log.info("  already in S3, skipping")
                    continue

                log.info("  downloading...")
                download_file(f"{BASE_URL}/{file_name}", local_path)

                log.info("  uploading to s3://%s/%s", BUCKET, s3_key)
                s3.upload_file(str(local_path), BUCKET, s3_key)
                log.info("  done")

    log.info("All files ingested.")


if __name__ == "__main__":
    run()