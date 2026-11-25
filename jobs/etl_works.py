#!/usr/bin/env python3
"""
Pipeline ETL PySpark da PoC Laguna On-Premises.
Lê recortes JSONL.gz do bucket raw-data via S3A, aplica as regras modulares em
jobs/rules.py e materializa a saída em formato Apache Parquet no bucket clean-data,
registrando exclusivamente o tempo de processamento (separado do download).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DataType,
    IntegerType,
    MapType,
    NullType,
    StringType,
    StructField,
    StructType,
)
from rules import apply_laguna_rules
from spark_session import build_spark_session


def sanitize_parquet_type(dtype: DataType) -> DataType:
    """Converte NullType residual em StringType para compatibilidade com Apache Parquet."""
    if isinstance(dtype, NullType):
        return StringType()
    if isinstance(dtype, StructType):
        fields = [
            StructField(f.name, sanitize_parquet_type(f.dataType), f.nullable)
            for f in dtype.fields
            if f.name != "_corrupt_record"
        ]
        return StructType(fields) if fields else StringType()
    if isinstance(dtype, ArrayType):
        return ArrayType(sanitize_parquet_type(dtype.elementType), dtype.containsNull)
    if isinstance(dtype, MapType):
        return MapType(
            sanitize_parquet_type(dtype.keyType),
            sanitize_parquet_type(dtype.valueType),
            dtype.valueContainsNull,
        )
    return dtype


def infer_openalex_works_schema(spark: SparkSession, input_path: str, sample_rows: int = 1000) -> StructType:
    """
    Infere o esquema da entidade Works preservando todos os atributos e tipando
    explicitamente 'abstract_inverted_index' como MapType(StringType(), ArrayType(IntegerType())),
    evitando colisões de tokens dinâmicos e caracteres inválidos em nomes de colunas Parquet.

    Nota metodológica (TCC 1 -> TCC 2): nesta fase de homologação neutra, a amostragem
    de esquema integra a janela medida de ETL. Na etapa experimental do TCC 2 (Atividade A07),
    com o inventário oficial das colunas do Projeto Laguna consolidado, o esquema alvo será
    declarado estaticamente, eliminando a leitura prévia de amostragem no S3A.
    """
    raw_sample = (
        spark.read.option("recursiveFileLookup", "true")
        .text(input_path)
        .limit(sample_rows)
        .collect()
    )
    stripped_lines = []
    for row in raw_sample:
        if not row.value:
            continue
        obj = json.loads(row.value)
        obj.pop("abstract_inverted_index", None)
        stripped_lines.append(json.dumps(obj))

    rdd = spark.sparkContext.parallelize(stripped_lines, numSlices=2)
    base_schema = sanitize_parquet_type(spark.read.json(rdd).schema)
    assert isinstance(base_schema, StructType)

    fields = [f for f in base_schema.fields if f.name != "abstract_inverted_index"]
    fields.append(
        StructField(
            "abstract_inverted_index",
            MapType(StringType(), ArrayType(IntegerType()), True),
            True,
        )
    )
    return StructType(fields)


def main() -> int:
    parser = argparse.ArgumentParser(description="Executa pipeline ETL Works sobre S3A local")
    parser.add_argument("--input-prefix", default="works/subset", help="Prefixo no bucket raw-data")
    parser.add_argument("--output-prefix", default="works/clean", help="Prefixo no bucket clean-data")
    parser.add_argument("--subset-manifest", default="/opt/laguna/results/subset_manifest.json")
    parser.add_argument("--metrics-out", default="/opt/laguna/results/last_etl_metrics.json")
    args = parser.parse_args()

    bucket_raw = os.environ.get("S3_BUCKET_RAW", "raw-data")
    bucket_clean = os.environ.get("S3_BUCKET_CLEAN", "clean-data")
    input_path = f"s3a://{bucket_raw}/{args.input_prefix}"
    output_path = f"s3a://{bucket_clean}/{args.output_prefix}"

    uncompressed_mb = None
    if args.subset_manifest and os.path.exists(args.subset_manifest):
        try:
            with open(args.subset_manifest, encoding="utf-8") as mf:
                sub = json.load(mf)
                if sub.get("total_uncompressed_bytes"):
                    uncompressed_mb = sub["total_uncompressed_bytes"] / (1024 * 1024)
        except Exception:
            uncompressed_mb = None

    spark = build_spark_session("laguna-poc-etl-works")
    try:
        t0 = time.perf_counter()

        works_schema = infer_openalex_works_schema(spark, input_path)
        df_raw = (
            spark.read.schema(works_schema)
            .option("recursiveFileLookup", "true")
            .json(input_path)
        )
        df_clean = apply_laguna_rules(df_raw)

        # Ação Spark que materializa integralmente a saída (fim real da avaliação preguiçosa)
        df_clean.write.mode("overwrite").parquet(output_path)

        elapsed_s = round(time.perf_counter() - t0, 3)

        # Leitura da saída materializada para contabilização exata
        df_out = spark.read.parquet(output_path)
        output_records = df_out.count()
        throughput_rec_s = round(output_records / elapsed_s, 2) if elapsed_s > 0 else 0.0
        throughput_mb_s = (
            round(uncompressed_mb / elapsed_s, 3)
            if (uncompressed_mb is not None and elapsed_s > 0)
            else None
        )

        metrics = {
            "input_path": input_path,
            "output_path": output_path,
            "execution_time_seconds": elapsed_s,
            "output_records": output_records,
            "uncompressed_input_mb": round(uncompressed_mb, 3) if uncompressed_mb is not None else None,
            "throughput_records_per_second": throughput_rec_s,
            "throughput_mb_per_second": throughput_mb_s,
            "output_column_count": len(df_out.columns),
            "output_columns": df_out.columns,
        }

        out_dir = os.path.dirname(args.metrics_out)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(args.metrics_out, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False)

        print(json.dumps(metrics, indent=2, ensure_ascii=False))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
