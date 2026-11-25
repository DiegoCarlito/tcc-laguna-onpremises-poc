#!/usr/bin/env python3
"""Inicializa de forma idempotente os buckets S3 da PoC no SeaweedFS."""
import os
import sys
import time
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError


def wait_for_s3(client, retries: int = 30, delay: float = 2.0) -> None:
    for attempt in range(1, retries + 1):
        try:
            client.list_buckets()
            return
        except (EndpointConnectionError, ClientError) as exc:
            print(f"[init-buckets] Aguardando gateway S3 ({attempt}/{retries}): {exc}")
            time.sleep(delay)
    raise RuntimeError("Gateway S3 não respondeu dentro do tempo limite.")


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
        print(f"[init-buckets] Bucket já existente: {bucket}")
    except ClientError:
        client.create_bucket(Bucket=bucket)
        print(f"[init-buckets] Bucket criado: {bucket}")


def main() -> int:
    endpoint = os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
    access_key = os.environ.get("S3_ACCESS_KEY", "laguna_poc_access")
    secret_key = os.environ.get("S3_SECRET_KEY", "laguna_poc_secret")
    buckets = [
        os.environ.get("S3_BUCKET_RAW", "raw-data"),
        os.environ.get("S3_BUCKET_CLEAN", "clean-data"),
    ]

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )

    wait_for_s3(client)
    for bucket in buckets:
        ensure_bucket(client, bucket)
    print("[init-buckets] Inicialização concluída com sucesso.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
