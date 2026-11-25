#!/usr/bin/env python3
"""
Ingere um recorte controlado da entidade Works do snapshot público da OpenAlex
para o bucket raw-data do armazenamento S3 local em modo streaming (chunks),
evitando consumo excessivo de memória RAM em partições grandes e registrando
manifesto de reprodutibilidade (data, objetos, SHA-256, bytes e tempo).
"""
from __future__ import annotations

import argparse
import datetime
import gzip
import hashlib
import json
import os
import sys
import tempfile
import time
import boto3
import requests

OPENALEX_MANIFEST_URL = "https://openalex.s3.amazonaws.com/data/jsonl/works/manifest.json"
OPENALEX_BASE_HTTPS = "https://openalex.s3.amazonaws.com/"
CHUNK_SIZE = 1024 * 1024  # 1 MiB


def s3_uri_to_https(uri: str) -> str:
    if uri.startswith("s3://openalex/"):
        return OPENALEX_BASE_HTTPS + uri[len("s3://openalex/"):]
    return uri


def extract_relative_key(uri: str) -> str:
    for marker in ("data/jsonl/works/", "data/works/"):
        if marker in uri:
            return uri.split(marker, 1)[-1]
    return os.path.basename(uri)


def fetch_manifest() -> dict:
    resp = requests.get(OPENALEX_MANIFEST_URL, timeout=60)
    resp.raise_for_status()
    return resp.json()


def stream_download_and_inspect(url: str, tmp_path: str) -> tuple[int, int, str]:
    """Baixa o arquivo .gz em chunks para disco, calculando SHA-256 e tamanho descomprimido."""
    hasher = hashlib.sha256()
    compressed_bytes = 0
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(tmp_path, "wb") as out_f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                out_f.write(chunk)
                hasher.update(chunk)
                compressed_bytes += len(chunk)

    uncompressed_bytes = 0
    with gzip.open(tmp_path, "rb") as gz_f:
        while True:
            decompressed_chunk = gz_f.read(CHUNK_SIZE)
            if not decompressed_chunk:
                break
            uncompressed_bytes += len(decompressed_chunk)

    return compressed_bytes, uncompressed_bytes, hasher.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingestão controlada de recorte Works da OpenAlex")
    parser.add_argument("--limit", type=int, default=1, help="Número de partições .gz a baixar")
    parser.add_argument("--min-records", type=int, default=1, help="Mínimo de registros por partição selecionada")
    parser.add_argument("--prefix", default="works/subset", help="Prefixo de destino no bucket raw-data")
    parser.add_argument("--output-manifest", default="/opt/laguna/results/subset_manifest.json")
    args = parser.parse_args()

    endpoint = os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
    access_key = os.environ.get("S3_ACCESS_KEY", "laguna_poc_access")
    secret_key = os.environ.get("S3_SECRET_KEY", "laguna_poc_secret")
    bucket_raw = os.environ.get("S3_BUCKET_RAW", "raw-data")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )

    print(f"[ingest] Baixando manifesto oficial: {OPENALEX_MANIFEST_URL}")
    manifest = fetch_manifest()
    raw_items = manifest.get("files", manifest.get("entries", []))
    entries = [
        e for e in raw_items
        if e.get("meta", {}).get("content_length", 0) > 0
        and e.get("meta", {}).get("record_count", 0) >= args.min_records
    ]
    # Ordena por tamanho crescente para testes progressivos reprodutíveis
    entries_sorted = sorted(entries, key=lambda e: e.get("meta", {}).get("content_length", 0))
    selected = entries_sorted[: args.limit]

    t0 = time.perf_counter()
    ingested = []
    for idx, entry in enumerate(selected, 1):
        src_url = s3_uri_to_https(entry["url"])
        rel_name = extract_relative_key(entry["url"])
        dest_key = f"{args.prefix}/{rel_name}"

        print(f"[ingest] ({idx}/{len(selected)}) Baixando em streaming {src_url} ...")
        fd, tmp_path = tempfile.mkstemp(suffix=".gz", prefix="openalex_part_")
        os.close(fd)
        try:
            compressed_bytes, uncompressed_bytes, sha256 = stream_download_and_inspect(
                src_url, tmp_path
            )
            s3.upload_file(tmp_path, bucket_raw, dest_key)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        ingested.append({
            "source_url": src_url,
            "s3_key": dest_key,
            "compressed_bytes": compressed_bytes,
            "uncompressed_bytes": uncompressed_bytes,
            "record_count_manifest": entry.get("meta", {}).get("record_count"),
            "sha256": sha256,
        })

    elapsed = round(time.perf_counter() - t0, 3)
    out = {
        "captured_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source_manifest_url": OPENALEX_MANIFEST_URL,
        "snapshot_date": manifest.get("date"),
        "snapshot_format": manifest.get("format", "jsonl"),
        "snapshot_total_works_records": manifest.get("record_count"),
        "snapshot_total_compressed_bytes": manifest.get("content_length"),
        "selection_strategy": f"smallest_partitions_first (min_records>={args.min_records})",
        "bucket": bucket_raw,
        "prefix": args.prefix,
        "download_seconds": elapsed,
        "total_compressed_bytes": sum(x["compressed_bytes"] for x in ingested),
        "total_uncompressed_bytes": sum(x["uncompressed_bytes"] for x in ingested),
        "total_manifest_records": sum(x["record_count_manifest"] or 0 for x in ingested),
        "objects": ingested,
    }

    out_dir = os.path.dirname(args.output_manifest)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(args.output_manifest, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"[ingest] Concluído em {elapsed}s ({len(ingested)} objeto(s)). Manifesto salvo em {args.output_manifest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
