#!/usr/bin/env python3
"""
Teste mínimo de conectividade end-to-end entre Apache Spark (Standalone),
conector S3AFileSystem e armazenamento de objetos SeaweedFS.
"""
import os
import sys
import time
from spark_session import build_spark_session


def main() -> int:
    bucket_clean = os.environ.get("S3_BUCKET_CLEAN", "clean-data")
    target_path = f"s3a://{bucket_clean}/_smoke_test/parquet_check"

    spark = build_spark_session("laguna-poc-smoke-test-s3a")
    try:
        t0 = time.perf_counter()
        sample = [
            ("W0001", "Teste de Conectividade S3A", 2026),
            ("W0002", "Validacao Spark Standalone", 2026),
            ("W0003", "Escrita Parquet SeaweedFS", 2026),
        ]
        df = spark.createDataFrame(sample, ["id", "title", "year"])
        df.write.mode("overwrite").parquet(target_path)

        df_read = spark.read.parquet(target_path)
        count = df_read.count()
        elapsed = round(time.perf_counter() - t0, 3)

        assert count == len(sample), f"Contagem divergente: esperado {len(sample)}, obtido {count}"
        print(f"[smoke-test] OK: escrita e leitura Parquet via S3A em {elapsed}s ({count} registros).")
        df_read.show(truncate=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
