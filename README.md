# Laguna On-Premises PoC

Prova de Conceito (PoC) de processamento local (*on-premises*) de recortes da entidade *Works* da base **OpenAlex** no contexto do **Projeto Laguna** (UnB / IBICT, LaePeCBr), utilizando **Apache Spark 3.5.3 (Standalone / PySpark)**, **SeaweedFS 3.79 (API S3)** e **Docker Compose v2**.

## Objetivo

Avaliar a viabilidade técnica de reproduzir localmente, em uma única máquina hospedeira conteinerizada, o fluxo de extração, transformação e carga (ETL) de recortes controlados da entidade *Works* da OpenAlex, verificando:

- **P1 (Correção funcional):** conformidade de contagem de registros, identificadores, esquema e consultas de validação frente a uma saída de referência;
- **P2 (Desempenho e consumo de recursos):** tempo total de execução (s), vazão (registros/s e MB/s), pico de memória RAM (GB e %), utilização de CPU (%) e taxa de I/O de disco (MB/s).

## Ambiente Hospedeiro e Pilha Verificada

- **Sistema operacional:** Ubuntu 24.04.5 LTS (`Linux 6.8.0-151-generic`, `x86_64`)
- **CPU:** Intel Core i5-1135G7 @ 2,40 GHz (até 4,20 GHz, 4 núcleos físicos / 8 *threads* lógicos)
- **Memória RAM:** 7,5 GiB utilizáveis (8 GB nominais; soma de `mem_limit` dos contêineres calibrada em 5,5 GiB, abaixo de 80% da RAM física)
- **Armazenamento:** SSD NVMe Western Digital PC SN530 (`WDC PC SN530 SDBPNPZ-512G-1114`, 476,9 GiB, `ext4`) e SSD SATA secundário Kingston A400 (`SA400S37240G`, 223,6 GiB)
- **Orquestração:** Docker Engine `27.2.0` e Docker Compose `v2.29.2`
- **Imagens base e *digests* SHA-256:**
  - `apache/spark:3.5.3-python3` (OpenJDK `11.0.24`, Python `3.8.10`, `hadoop-aws` `3.3.4`, `aws-java-sdk-bundle` `1.12.262`): `sha256:d0f5d63bfbbffdbec08debb4911554ba671a2fa7398374db8db314d80328f576` (`linux/amd64`: `sha256:b2da01c5855fdf791328a6fa1267b406336a535d39abc05699214a48bee95955`)
  - `chrislusf/seaweedfs:3.79`: `sha256:21425305e04b355904ef350ff75a4e81b8afc5124f63e0485b946164b58562d3`

## Estrutura do Repositório

```text
laguna-onpremises-poc/
├── .env.example                 # Template de variáveis e limites de CPU/RAM
├── docker-compose.yml           # Orquestração: SeaweedFS, Spark Master/Workers e Driver
├── docker/
│   ├── seaweedfs/s3.json        # Credenciais locais do gateway S3
│   └── spark/
│       ├── Dockerfile           # Imagem PySpark 3.5.3 + hadoop-aws 3.3.4 (S3A)
│       └── spark-defaults.conf  # Parâmetros S3A, memória e conectividade Driver-Executors
├── jobs/
│   ├── spark_session.py         # Fábrica padronizada de SparkSession com S3A
│   ├── smoke_test_s3a.py        # Teste mínimo de conectividade Spark <-> S3A
│   ├── rules.py                 # Módulo neutro de transformação (a alinhar ao inventário Laguna)
│   ├── etl_works.py             # Pipeline ETL PySpark cronometrado com tipagem MapType
│   ├── validate_output.py       # Conferência funcional de saídas (contagem, IDs, esquema)
│   └── sample_analytics.py      # Consulta analítica de exemplo em Spark SQL (clean-data)
├── scripts/
│   ├── init_buckets.py          # Inicialização idempotente de raw-data e clean-data
│   ├── ingest_openalex_subset.py# Ingestão de recorte JSONL.gz (data/jsonl/works/manifest.json)
│   ├── capture_host_info.py     # Manifesto de reprodutibilidade (SO, CPU, RAM, disco, digests)
│   └── run_benchmark.py         # Orquestrador de repetições e coleta de métricas
└── requirements.txt             # Dependências Python dos scripts auxiliares
```

## Execução Rápida

1. Configure o ambiente local:
   ```bash
   cp .env.example .env
   ```
2. Construa e suba o *cluster* local:
   ```bash
   docker compose up -d --build
   ```
3. Capture o manifesto do hospedeiro (SO, CPU, RAM, tipo de disco e *digests* das imagens):
   ```bash
   python3 scripts/capture_host_info.py --output results/host_manifest.json
   ```
4. Execute o teste de conectividade Spark e SeaweedFS (`S3AFileSystem`):
   ```bash
   docker compose exec -T spark-driver /opt/spark/bin/spark-submit \
     --driver-memory 512m --executor-memory 512m /opt/laguna/jobs/smoke_test_s3a.py
   ```
5. Realize a ingestão de um recorte controlado da OpenAlex (separada da medição do ETL):
   ```bash
   docker compose exec -T spark-driver python3 /opt/laguna/scripts/ingest_openalex_subset.py \
     --limit 2 --min-records 200 --output-manifest /opt/laguna/results/subset_manifest.json
   ```
6. Execute o pipeline ETL e a validação funcional sobre o recorte ingerido:
   ```bash
   docker compose exec -T spark-driver /opt/spark/bin/spark-submit \
     --driver-memory 512m --executor-memory 512m /opt/laguna/jobs/etl_works.py \
     --input-prefix works/subset --output-prefix works/clean \
     --metrics-out /opt/laguna/results/last_etl_metrics.json

   docker compose exec -T spark-driver /opt/spark/bin/spark-submit \
     --driver-memory 512m --executor-memory 512m /opt/laguna/jobs/validate_output.py \
     --clean-prefix works/clean --report-out /opt/laguna/results/validation_report.json

   docker compose exec -T spark-driver /opt/spark/bin/spark-submit \
     --driver-memory 512m --executor-memory 512m /opt/laguna/jobs/sample_analytics.py \
     --clean-prefix works/clean --report-out /opt/laguna/results/sample_analytics.json
   ```
7. Execute o protocolo de benchmark com repetições:
   ```bash
   python3 scripts/run_benchmark.py --runs 3 --output results/benchmark_runs.csv
   ```

## Evidência de Verificação Inicial

- **Smoke test S3A (`smoke_test_s3a.py`):** escrita e leitura Parquet em `s3a://clean-data/_smoke_test/parquet_check` concluídas em `8,129 s` (`3` registros) nos dois *Workers*.
- **Ingestão (`ingest_openalex_subset.py`):** download de `2` partições do *snapshot* `2026-10-14` (`693` registros; `501,24 KB` comprimidos; `4,755 MB` descomprimidos) para `s3a://raw-data/works/subset` em `3,383 s`.
- **ETL e validação (`etl_works.py` + `validate_output.py`):** materialização Parquet (Snappy) em `s3a://clean-data/works/clean` em `12,274 s` (`56,46 registros/s`; `0,387 MB/s`), preservando `693` registros únicos (`100%` de unicidade de `id`) e `51` colunas de nível superior.

## Observação Metodológica

As transformações presentes em `jobs/rules.py` correspondem a uma estrutura modular neutra inicial para validação técnica do conector S3A e do fluxo de ETL. As regras definitivas de higienização e particionamento serão alinhadas ao inventário oficial dos *scripts* do Projeto Laguna na etapa de execução do TCC 2.
