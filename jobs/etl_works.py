#!/usr/bin/env python3
"""
Pipeline ETL PySpark da PoC Laguna On-Premises.
Lê recortes JSONL.gz do bucket raw-data via S3A, aplica as regras modulares em
jobs/rules.py e materializa a saída em formato Apache Parquet no bucket clean-data,
registrando exclusivamente o tempo de processamento (separado do download).
"""
import argparse
import json
import os
import sys
import time
from rules import apply_laguna_rules
from spark_session import build_spark_session


def main() -> int:
    parser = argparse.ArgumentParser(description="Executa pipeline ETL Works sobre S3A local")
    parser.add_argument("--input-prefix", default="works/subset", help="Prefixo no bucket raw-data")
    parser.add_argument("--output-prefix", default="works/clean", help="Prefixo no bucket clean-data")
    parser.add_argument("--metrics-out", default="/opt/laguna/results/last_etl_metrics.json")
    args = parser.parse_args()

    bucket_raw = os.environ.get("S3_BUCKET_RAW", "raw-data")
    bucket_clean = os.environ.get("S3_BUCKET_CLEAN", "clean-data")
    input_path = f"s3a://{bucket_raw}/{args.input_prefix}"
    output_path = f"s3a://{bucket_clean}/{args.output_prefix}"

    spark = build_spark_session("laguna-poc-etl-works")
    try:
        t0 = time.perf_counter()

        df_raw = spark.read.json(input_path)
        df_clean = apply_laguna_rules(df_raw)

        # Ação Spark que materializa integralmente a saída (fim real da avaliação preguiçosa)
        df_clean.write.mode("overwrite").parquet(output_path)

        elapsed_s = round(time.perf_counter() - t0, 3)

        # Leitura da saída materializada para contabilização exata
        df_out = spark.read.parquet(output_path)
        output_records = df_out.count()
        throughput_rec_s = round(output_records / elapsed_s, 2) if elapsed_s > 0 else 0.0

        metrics = {
            "input_path": input_path,
            "output_path": output_path,
            "execution_time_seconds": elapsed_s,
            "output_records": output_records,
            "throughput_records_per_second": throughput_rec_s,
            "output_columns": df_out.columns,
        }

        os.makedirs(os.path.dirname(args.metrics_out), exist_ok=True)
        with open(args.metrics_out, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False)

        print(json.dumps(metrics, indent=2, ensure_ascii=False))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
