# Laguna On-Premises PoC

Prova de Conceito (PoC) de processamento local (*on-premises*) de recortes da entidade *Works* da base **OpenAlex** no contexto do **Projeto Laguna** (UnB / IBICT, LaePeCBr), utilizando **Apache Spark 3.5.3 (Standalone / PySpark)**, **SeaweedFS 3.79 (API S3)** e **Docker Compose v2**.

## Objetivo

Avaliar a viabilidade técnica de reproduzir localmente, em uma única máquina hospedeira conteinerizada, o fluxo de extração, transformação e carga (ETL) de recortes controlados da entidade *Works* da OpenAlex, verificando:

- **P1 (Correção funcional):** conformidade de contagem de registros, identificadores, esquema e consultas de validação frente a uma saída de referência;
- **P2 (Desempenho e consumo de recursos):** tempo total de execução (s), vazão (registros/s e MB/s), pico de memória RAM (GB e %), utilização de CPU (%) e taxa de I/O de disco (MB/s).

## Ambiente Hospedeiro e Pilha Verificada

- **Sistema operacional:** Ubuntu 24.04.5 LTS (`Linux 6.8.0-151-generic`, `x86_64`)
- **CPU:** Intel Core i5-1135G7 @ 2,40 GHz (até 4,20 GHz, 4 núcleos físicos / 8 *threads* lógicos)
- **Memória RAM:** 7,5 GiB utilizáveis (8 GB nominais; soma de `mem_limit` dos contêineres calibrada em 5,5 GiB, equivalente a 73,3% da RAM física e abaixo do teto de 80%)
- **Armazenamento:** Unidade SSD NVMe Western Digital PC SN530 (`WDC PC SN530 SDBPNPZ-512G-1114`, 476,9 GiB, sistema de arquivos `ext4`)
- **Orquestração:** Docker Engine `27.2.0` (nativo sobre o *kernel* Linux) e Docker Compose `v2.29.2`
- **Imagens base e *digests* SHA-256:**
  - `apache/spark:3.5.3-python3` (OpenJDK `11.0.24`, Python `3.8.10`, `hadoop-aws` `3.3.4`, `aws-java-sdk-bundle` `1.12.262`): `sha256:d0f5d63bfbbffdbec08debb4911554ba671a2fa7398374db8db314d80328f576` (`linux/amd64`: `sha256:b2da01c5855fdf791328a6fa1267b406336a535d39abc05699214a48bee95955`)
  - `chrislusf/seaweedfs:3.79`: `sha256:21425305e04b355904ef350ff75a4e81b8afc5124f63e0485b946164b58562d3`

### Orçamento de Recursos por Contêiner

| Serviço | Papel | vCPUs (`cpus`) | Limite RAM (`mem_limit`) | Alocação Interna / JVM |
|---|---|---:|---:|---|
| `seaweedfs` | Armazenamento de objetos (API S3) | `1.0` | `512m` | Limite de volume: `1024 MB` |
| `spark-master` | *Cluster Manager* (*Standalone*) | `1.0` | `512m` | `SPARK_DAEMON_MEMORY=96m` (`SerialGC`) |
| `spark-driver` | Submissão PySpark (*client mode*) | `2.0` | `1536m` | `SPARK_DRIVER_MEMORY=512m` |
| `spark-worker-1` | Nó de execução (*Executor* 1) | `2.0` | `1536m` | `WORKER=640m`, `EXECUTOR=512m`, `DAEMON=96m` |
| `spark-worker-2` | Nó de execução (*Executor* 2) | `2.0` | `1536m` | `WORKER=640m`, `EXECUTOR=512m`, `DAEMON=96m` |
| **Total** | **5 serviços de longa duração** | **`8.0`** | **`5,5 GiB`** | **73,3% dos 7,5 GiB físicos (< 80%)** |

> **Decisão de engenharia (execução via `spark-submit` vs. Jupyter Notebook):** os *pipelines* e *benchmarks* são executados por *scripts* Python modulares submetidos via `spark-submit` (sem servidor Jupyter residente), reproduzindo o modelo de execução em lote não interativo (`AWS Batch` / `EMR`) e evitando sobrecarga adicional de memória RAM ou retenção de sessão Spark entre repetições experimentais.

## Estrutura do Repositório

```text
laguna-onpremises-poc/
├── .env.example                 # Template calibrado de variáveis e limites de CPU/RAM
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
│   ├── ingest_openalex_subset.py# Ingestão em streaming de recorte JSONL.gz (manifest.json)
│   ├── capture_host_info.py     # Manifesto de reprodutibilidade (SO, CPU, RAM, disco, digests)
│   └── run_benchmark.py         # Orquestrador de repetições, limpeza e coleta de métricas
├── results/
│   ├── host_manifest.json       # Especificação verificada do host e digests das imagens
│   ├── subset_manifest.json     # Manifesto do recorte ingerido (snapshot, objetos, SHA-256)
│   ├── last_etl_metrics.json    # Tempo, vazão e colunas do teste preliminar de ETL
│   ├── validation_report.json   # Relatório de unicidade de IDs e esquema Parquet (51 colunas)
│   └── sample_analytics.json    # Saída das 3 consultas agregadas em Spark SQL
├── LICENSE                      # Licença aberta Apache License 2.0
└── requirements.txt             # Dependências Python dos scripts auxiliares
```

## Execução Rápida

> **Pré-requisitos no hospedeiro:** Docker Engine, Docker Compose v2 e Python `3.8+` (apenas biblioteca padrão para `capture_host_info.py` e `run_benchmark.py`). As bibliotecas externas listadas em `requirements.txt` (`boto3` e `requests`) são instaladas automaticamente dentro da imagem `laguna-spark` durante o `docker compose up -d --build`. Caso altere `S3_ACCESS_KEY` ou `S3_SECRET_KEY` no `.env`, atualize também `docker/seaweedfs/s3.json`.

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
5. Realize a ingestão em *streaming* de um recorte controlado da OpenAlex (separada da medição do ETL):
   ```bash
   docker compose exec -T spark-driver python3 /opt/laguna/scripts/ingest_openalex_subset.py \
     --limit 2 --min-records 200 --output-manifest /opt/laguna/results/subset_manifest.json
   ```
6. Execute o pipeline ETL, a validação funcional e as consultas analíticas sobre o recorte ingerido:
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
7. Execute o protocolo de benchmark com repetições e limpeza automática de artefatos entre rodadas:
   ```bash
   python3 scripts/run_benchmark.py --runs 3 --output results/benchmark_runs.csv
   ```
   O arquivo CSV consolidado exporta as colunas `run_id`, `status`, `execution_time_s`, `output_records`, `throughput_rec_s`, `throughput_mb_s`, `peak_ram_gb`, `peak_ram_pct`, `avg_cpu_pct`, `peak_cpu_pct` e `disk_io_mb_s` (adicione `--drop-caches` quando executado como `root` para descartar também o *page cache* do sistema operacional entre repetições).

### Encerramento e Limpeza

- Parar os contêineres preservando os *buckets* S3 e volumes locais:
  ```bash
  docker compose stop
  ```
- Remover contêineres e rede (mantendo os diretórios em `./volumes`):
  ```bash
  docker compose down
  ```

## Evidência de Verificação Inicial (`results/`)

- **Smoke test S3A (`smoke_test_s3a.py`):** escrita e leitura Parquet em `s3a://clean-data/_smoke_test/parquet_check` concluídas em `8,129 s` (`3` registros) nos dois *Workers*.
- **Ingestão (`results/subset_manifest.json`):** download de `2` partições do *snapshot* `2026-10-14` (`693` registros; `489,49 KiB` comprimidos; `4,755 MiB` descomprimidos) para `s3a://raw-data/works/subset` em `3,383 s`.
- **ETL e validação (`results/last_etl_metrics.json` + `results/validation_report.json`):** materialização Parquet (Snappy) em `s3a://clean-data/works/clean` em `12,274 s` (`56,46 registros/s`; `0,387 MiB/s`), preservando `693` registros únicos (`100%` de unicidade de `id`) e `51` colunas de nível superior.
- **Consulta analítica (`results/sample_analytics.json`):** agregações Spark SQL sobre `s3a://clean-data/works/clean` (distribuição por `publication_year`, `type` e `primary_location.source` com soma de `cited_by_count`) concluídas em `17,482 s` (`632` artigos, `41` livros e demais tipologias).

## Observação Metodológica

As transformações presentes em `jobs/rules.py` correspondem a uma estrutura modular neutra inicial para validação técnica do conector S3A e do fluxo de ETL. As regras definitivas de higienização e particionamento serão alinhadas ao inventário oficial dos *scripts* do Projeto Laguna na etapa de execução do TCC 2.

## Contexto Acadêmico

Artefato técnico integrante do Trabalho de Conclusão de Curso 1 (TCC 1) em Engenharia de Software da Universidade de Brasília (UnB / FCTE-FGA):
- **Título:** *Prova de Conceito de Processamento Local de Dados OpenAlex no Projeto Laguna com Apache Spark*
- **Autor:** Diego Carlito Rodrigues de Souza
- **Orientador:** Prof. Dr. Daniel Sundfeld Lima
- **Ano:** 2026
- **Licença:** distribuído sob a licença aberta [Apache License 2.0](LICENSE), alinhada ao licenciamento do Apache Spark, Apache Hadoop e SeaweedFS.
