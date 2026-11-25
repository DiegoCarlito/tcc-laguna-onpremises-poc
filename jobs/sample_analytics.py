#!/usr/bin/env python3
"""
Consulta analítica de exemplo em Spark SQL sobre a camada clean-data (Parquet),
demonstrando o consumo dos dados processados da entidade Works da OpenAlex.
"""
import argparse
import json
import os
import sys
import time
from spark_session import build_spark_session


def main() -> int:
    parser = argparse.ArgumentParser(description="Consulta analítica de exemplo sobre clean-data")
    parser.add_argument("--clean-prefix", default="works/clean")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--report-out", default="/opt/laguna/results/sample_analytics.json")
    args = parser.parse_args()

    bucket_clean = os.environ.get("S3_BUCKET_CLEAN", "clean-data")
    clean_path = f"s3a://{bucket_clean}/{args.clean_prefix}"

    spark = build_spark_session("laguna-poc-sample-analytics")
    try:
        t0 = time.perf_counter()
        df = spark.read.parquet(clean_path)
        df.createOrReplaceTempView("works_clean")
        cols = set(df.columns)

        summary = {
            "clean_path": clean_path,
            "total_works": df.count(),
            "by_publication_year": [],
            "by_type": [],
            "top_sources": [],
        }

        if "publication_year" in cols:
            rows = spark.sql(
                f"""
                SELECT publication_year, COUNT(*) AS works_count
                FROM works_clean
                WHERE publication_year IS NOT NULL
                GROUP BY publication_year
                ORDER BY works_count DESC, publication_year DESC
                LIMIT {int(args.top_n)}
                """
            ).collect()
            summary["by_publication_year"] = [
                {"publication_year": int(r["publication_year"]), "works_count": int(r["works_count"])}
                for r in rows
            ]

        if "type" in cols:
            rows = spark.sql(
                f"""
                SELECT type, COUNT(*) AS works_count
                FROM works_clean
                WHERE type IS NOT NULL
                GROUP BY type
                ORDER BY works_count DESC
                LIMIT {int(args.top_n)}
                """
            ).collect()
            summary["by_type"] = [
                {"type": r["type"], "works_count": int(r["works_count"])}
                for r in rows
            ]

        if "primary_location" in cols:
            cited_expr = "COALESCE(SUM(cited_by_count), 0)" if "cited_by_count" in cols else "0"
            rows = spark.sql(
                f"""
                SELECT
                    primary_location.source.id AS source_id,
                    primary_location.source.display_name AS source_name,
                    COUNT(*) AS works_count,
                    {cited_expr} AS total_citations
                FROM works_clean
                WHERE primary_location.source.display_name IS NOT NULL
                GROUP BY primary_location.source.id, primary_location.source.display_name
                ORDER BY works_count DESC, total_citations DESC
                LIMIT {int(args.top_n)}
                """
            ).collect()
            summary["top_sources"] = [
                {
                    "source_id": r["source_id"],
                    "source_name": r["source_name"],
                    "works_count": int(r["works_count"]),
                    "total_citations": int(r["total_citations"]),
                }
                for r in rows
            ]

        summary["execution_time_seconds"] = round(time.perf_counter() - t0, 3)

        os.makedirs(os.path.dirname(args.report_out), exist_ok=True)
        with open(args.report_out, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
