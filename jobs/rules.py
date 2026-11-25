"""
Módulo de regras de transformação da entidade Works.

IMPORTANTE (Rigor Metodológico):
As regras definitivas de seleção/remoção de atributos, tratamento de nulos,
normalização de tipos e particionamento devem refletir o inventário oficial
dos scripts do Projeto Laguna (Atividade A08 do TCC 2).

Esta implementação provê apenas uma transformação mínima neutra (validação de
registros com identificador não nulo e deduplicação por 'id') para permitir
testes técnicos iniciais do pipeline sem assumir regras não confirmadas.
"""
from pyspark.sql import DataFrame


def apply_laguna_rules(df: DataFrame) -> DataFrame:
    """Aplica regras mínimas neutras até a consolidação do inventário oficial do Laguna."""
    if "id" in df.columns:
        df = df.filter(df["id"].isNotNull()).dropDuplicates(["id"])
    return df
