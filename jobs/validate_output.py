#!/usr/bin/env python3
"""
Valida a conformidade funcional das saídas materializadas em clean-data:
contagem de registros, unicidade de identificadores, esquema e consulta Spark SQL.
Quando um caminho de referência é informado, compara diretamente ambos os conjuntos.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from spark_session import build_spark_session


def main() -> int:
    parser = argparse.ArgumentParser(description="Validação funcional das saídas do ETL")
    parser.add_argument("--clean-prefix", default="works/clean")
    parser.add_argument("--reference-path", default=None, help="Caminho Parquet opcional de referência")
    parser.add_argument("--report-out", default="/opt/laguna/results/validation_report.json")
    args = parser.parse_args()

    bucket_clean = os.environ.get("S3_BUCKET_CLEAN", "clean-data")
    clean_path = f"s3a://{bucket_clean}/{args.clean_prefix}"

    spark = build_spark_session("laguna-poc-validate-output")
    try:
        df = spark.read.parquet(clean_path)
        total_rows = df.count()
        distinct_ids = df.select("id").distinct().count() if "id" in df.columns else None
        schema_fields = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]

        report = {
            "clean_path": clean_path,
            "total_rows": total_rows,
            "distinct_ids": distinct_ids,
            "id_uniqueness_ok": (distinct_ids == total_rows) if distinct_ids is not None else None,
            "column_count": len(schema_fields),
            "schema": schema_fields,
            "reference_comparison": None,
        }

        if args.reference_path:
            df_ref = spark.read.parquet(args.reference_path)
            ref_rows = df_ref.count()
            ref_schema = [(f.name, f.dataType.simpleString()) for f in df_ref.schema.fields]
            missing_in_clean = df_ref.select("id").subtract(df.select("id")).count()
            extra_in_clean = df.select("id").subtract(df_ref.select("id")).count()
            report["reference_comparison"] = {
                "reference_path": args.reference_path,
                "reference_rows": ref_rows,
                "row_count_match": ref_rows == total_rows,
                "schema_match": ref_schema == schema_fields,
                "missing_ids_in_clean": missing_in_clean,
                "extra_ids_in_clean": extra_in_clean,
                "functional_conformity_100pct": (
                    ref_rows == total_rows
                    and ref_schema == schema_fields
                    and missing_in_clean == 0
                    and extra_in_clean == 0
                ),
            }

        out_dir = os.path.dirname(args.report_out)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(args.report_out, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
