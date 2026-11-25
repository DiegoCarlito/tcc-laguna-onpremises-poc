# Laguna On-Premises PoC

Prova de Conceito (PoC) de processamento local (*on-premises*) de recortes da entidade *Works* da base **OpenAlex** no contexto do **Projeto Laguna** (UnB / IBICT — LaePeCBr), utilizando **Apache Spark (Standalone / PySpark)**, **SeaweedFS (API S3)** e **Docker Compose**.

## Objetivo

Avaliar a viabilidade técnica de reproduzir localmente, em uma única máquina hospedeira conteinerizada, o fluxo de extração, transformação e carga (ETL) de recortes controlados da entidade *Works* da OpenAlex, verificando:

- **P1 (Correção funcional):** conformidade de contagem de registros, identificadores, esquema e consultas de validação frente a uma saída de referência;
- **P2 (Desempenho e consumo de recursos):** tempo total de execução (s), vazão (registros/s e MB/s), pico de memória RAM (GB e %), utilização de CPU (%) e taxa de I/O de disco (MB/s).

## Estrutura do Repositório

```text
laguna-onpremises-poc/
├── .env.example                 # Template de variáveis e limites de CPU/RAM
├── docker-compose.yml           # Orquestração: SeaweedFS, Spark Master/Workers e Driver
├── docker/
│   ├── seaweedfs/s3.json        # Credenciais locais do gateway S3
│   └── spark/
│       ├── Dockerfile           # Imagem PySpark 3.5.3 + hadoop-aws 3.3.4 (S3A)
│       └── spark-defaults.conf  # Parâmetros S3A e conectividade Driver-Executors
├── jobs/
│   ├── smoke_test_s3a.py        # Teste mínimo de conectividade Spark <-> S3A
│   ├── rules.py                 # Módulo de regras de transformação (a alinhar ao inventário Laguna)
│   ├── etl_works.py             # Pipeline ETL PySpark cronometrado
│   └── validate_output.py       # Conferência funcional de saídas (contagem, IDs, esquema)
├── scripts/
│   ├── init_buckets.py          # Inicialização idempotente de raw-data e clean-data
│   ├── ingest_openalex_subset.py# Ingestão de recorte .json.gz com manifesto SHA-256
│   ├── capture_host_info.py     # Manifesto de reprodutibilidade (SO, CPU, RAM, disco, digests)
│   └── run_benchmark.py         # Orquestrador de repetições e coleta de métricas
└── requirements.txt             # Dependências Python dos scripts auxiliares
```

## Pré-requisitos

- Linux (ex.: Ubuntu 24.04 LTS)
- Docker Engine (`>= 24.x`) e Docker Compose v2 (`>= 2.20`)
- Python 3.10+ (para scripts de host/coleta)

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
4. Execute o teste de conectividade Spark ↔ SeaweedFS (`S3AFileSystem`):
   ```bash
   docker compose exec spark-driver spark-submit /opt/laguna/jobs/smoke_test_s3a.py
   ```
5. Realize a ingestão de um recorte controlado da OpenAlex (separada da medição do ETL):
   ```bash
   docker compose exec spark-driver python3 /opt/laguna/scripts/ingest_openalex_subset.py \
     --limit 1 --output-manifest /opt/laguna/results/subset_manifest.json
   ```
6. Execute o protocolo de benchmark (ex.: 3 repetições):
   ```bash
   python3 scripts/run_benchmark.py --runs 3 --output results/benchmark_runs.csv
   ```

## Observação Metodológica

As transformações presentes em `jobs/rules.py` correspondem a uma estrutura modular inicial para validação técnica do conector S3A e do fluxo de ETL. As regras definitivas de higienização e particionamento serão alinhadas ao inventário oficial dos *scripts* do Projeto Laguna na etapa de execução do TCC 2.
