<div align="center">

<h1>Financial Data — End-to-End Platform</h1>

<p>
  <img alt="CI" src="https://github.com/your-org/data-end-to-end/actions/workflows/ci.yml/badge.svg"/>
  <img alt="Python 3.11" src="https://img.shields.io/badge/python-3.11-blue?style=flat-square&color=1f4fa0"/>
  <img alt="PySpark 3.5" src="https://img.shields.io/badge/PySpark-3.5-orange?style=flat-square"/>
  <img alt="Delta Lake 3.1" src="https://img.shields.io/badge/Delta_Lake-3.1-blue?style=flat-square&color=002056"/>
  <img alt="Terraform" src="https://img.shields.io/badge/IaC-Terraform-purple?style=flat-square"/>
  <img alt="License MIT" src="https://img.shields.io/badge/license-MIT-gold?style=flat-square&color=cdac80"/>
</p>

</div>

---

## Problema

Dados financeiros públicos de mercado de ações (OHLCV — Open, High, Low, Close, Volume) são valiosos para análises de risco, retorno e tendência, mas chegam em formato CSV bruto, sem qualidade garantida, sem histórico de transformação e sem governança. Este projeto resolve esse problema construindo uma **plataforma de dados end-to-end** no Azure que:

1. **Ingere** automaticamente dados do Kaggle via Azure Data Factory
2. **Transforma** em uma arquitetura medallion (bronze → silver → gold) via Databricks
3. **Expõe** via Synapse Analytics Serverless SQL com tabelas externas Delta Lake
4. **Visualiza** em Power BI com métricas de retorno, médias móveis e sinais de tendência

O dataset utilizado é o [`adhoppin/financial-data`](https://www.kaggle.com/datasets/adhoppin/financial-data/data) do Kaggle — dados históricos OHLCV de múltiplos ativos (AAPL, MSFT, GOOGL, AMZN, TSLA).

---

## Arquitetura

```
Kaggle API
    │  HTTP GET (ADF Copy Activity)
    ▼
ADLS Gen2 ── raw/ ── financial-data/*.csv
    │
    │  Databricks Medallion Workflow (diário 06:00 BRT)
    │
    ├─► bronze/financial_data/        Delta Lake  ← raw + metadata (_ingested_at, _source_file)
    │
    ├─► silver/financial_data/        Delta Lake  ← typed, deduplicated, enriched
    │         partitioned by Date                   (daily_return_pct, intraday_range, price_spread_pct)
    │
    └─► gold/financial_data/          Delta Lake  ← MA7, MA30, lag returns, cumulative return
        gold/daily_summary/           Delta Lake  ← aggregated daily metrics per symbol
              │
              │  Synapse Serverless SQL (External Tables)
              │
              └─► gold.vw_rolling_volatility
                  gold.vw_symbol_performance
                  gold.vw_data_quality_check
                        │
                        │  Power BI DirectQuery
                        │
                        └─► financial_dashboard.pbip
                            ├─ Market Overview (KPIs, price history, volume)
                            └─ Technical Analysis (OHLC, MA crossover, cumulative return)
```

> Diagrama completo com Mermaid e Sequence Diagram: [`docs/architecture/architecture.md`](docs/architecture/architecture.md)

### Stack

| Camada | Tecnologia |
|--------|-----------|
| Ingestão | Azure Data Factory (HTTP → ADLS copy, Managed Identity) |
| Armazenamento | Azure Data Lake Storage Gen2 (4 containers: raw/bronze/silver/gold) |
| Transformação | Azure Databricks (PySpark 3.5, Delta Lake 3.1, medallion architecture) |
| Analytics SQL | Azure Synapse Analytics Serverless SQL (external tables, Delta format) |
| Visualização | Power BI (PBIP format, DirectQuery, semantic model com DAX measures) |
| IaC | Terraform (modularizado: storage, adf, databricks, synapse) |
| CI | GitHub Actions (lint, PySpark unit tests com Java 11, terraform validate) |

---

## Estrutura do Repositório

```
data-end-to-end/
├── .github/workflows/ci.yml          # CI: lint + tests + terraform validate
├── adf/
│   ├── linked_service/               # ADLS Gen2 e HTTP Kaggle
│   ├── dataset/                      # Datasets source e sink
│   └── pipeline/pl_ingest_financial_data.json
├── databricks/
│   ├── notebooks/
│   │   ├── 00_setup_mounts.py        # Monta ADLS no DBFS
│   │   ├── 01_raw_to_bronze.py       # CSV → Delta bronze
│   │   ├── 02_bronze_to_silver.py    # Limpeza, tipagem, métricas derivadas
│   │   └── 03_silver_to_gold.py      # MA7, MA30, retornos, agregações
│   └── jobs/medallion_workflow.json  # Workflow multi-task Databricks
├── synapse/sql/
│   ├── 00_setup_database.sql         # DB + managed identity credential
│   ├── 01_create_external_data_sources.sql
│   ├── 02_create_external_tables.sql # bronze, silver, gold Delta → SQL
│   └── 03_create_analytics_views.sql # Views de negócio
├── powerbi/
│   ├── financial_model.SemanticModel/model.bim   # Semantic model (DAX, DirectQuery)
│   └── financial_dashboard.Report/report.json    # Layout das páginas
├── infra/                            # Terraform (root + 4 modules)
├── tests/unit/                       # PySpark unit tests (17 testes)
├── data/sample/financial_sample.csv  # 500 linhas para dev local
├── scripts/
│   ├── download_kaggle_data.py       # Download + upload para ADLS
│   └── _gen_sample.py                # Gerador de dados de amostra
└── docs/architecture/architecture.md # Diagramas Mermaid + 5 ADRs
```

---

## Decisões Técnicas

### Por que Delta Lake em todas as camadas?

Delta Lake oferece ACID transactions, schema evolution e time-travel. No Synapse Serverless SQL, basta declarar `FILE_FORMAT = DELTA` numa external table — o engine lê o transaction log e enxerga sempre o snapshot mais recente. Não há ETL adicional entre Databricks e Synapse.

### Por que Synapse Serverless (não Dedicated Pool)?

Zero custo em idle. Cobrança por TB escaneado. Para um dataset financeiro com ~500K linhas (escala do Kaggle), o custo mensal é de centavos. Se o volume crescer para bilhões de linhas, o gold já está particionado por `Symbol` — as queries do Power BI filtram por símbolo e data, então o Synapse lê apenas as partições relevantes.

### Por que PBIP (não .pbix)?

`.pbix` é binário — não é diffável em git. O formato `.pbip` (Power BI Project) armazena o semantic model em `model.bim` (JSON TMSL) e o layout em `report.json`. Toda mudança de medida DAX, cor ou visual aparece num diff legível no PR.

### Por que bronze em append e silver/gold em overwrite?

Bronze é o arquivo imutável da ingestão. Se a transformação para silver tiver um bug, reprocessamos `02_bronze_to_silver` sem precisar re-ingerir. O bronze nunca é sobrescrito — cada run de ingestão adiciona linhas com `_pipeline_run_ts` para rastreabilidade.

> Todos os ADRs completos em [`docs/architecture/architecture.md`](docs/architecture/architecture.md)

---

## Como Rodar

### Pré-requisitos

| Ferramenta | Versão | Observação |
|-----------|--------|-----------|
| Python | 3.11+ | |
| Java | 11+ | Obrigatório para PySpark local |
| Terraform | 1.6+ | Para provisionar infra |
| Azure CLI | 2.58+ | Autenticação Azure |
| Kaggle API | — | Conta em kaggle.com |

### 1. Clone e configure variáveis de ambiente

```bash
git clone https://github.com/your-org/data-end-to-end.git
cd data-end-to-end
cp .env.example .env
# Edite .env com suas credenciais Azure e Kaggle
```

### 2. Instale dependências Python

```bash
pip install -r requirements-dev.txt
```

### 3. Provisione a infraestrutura Azure

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
# Edite terraform.tfvars com sua senha Synapse
az login
terraform init
terraform apply
cd ..
```

Os outputs do Terraform fornecem os valores para preencher o `.env` (`ADLS_ACCOUNT_NAME`, `DATABRICKS_HOST`, `SYNAPSE_SQL_ENDPOINT`).

### 4. Baixe os dados do Kaggle e envie para ADLS

```bash
# Certifique-se de ter KAGGLE_USERNAME e KAGGLE_KEY no .env
make download-data
```

### 5. Publique os artefatos ADF

```bash
make deploy-adf
```

### 6. Importe os notebooks no Databricks

No workspace Databricks, use **Repos → Add Repo** apontando para este repositório GitHub. Os notebooks em `databricks/notebooks/` serão sincronizados automaticamente.

Importe o workflow:
```bash
databricks jobs create --json @databricks/jobs/medallion_workflow.json
```

### 7. Execute o pipeline pela primeira vez

```bash
make run-pipeline
# Ou via ADF UI: pl_ingest_financial_data → Add Trigger → Trigger Now
```

### 8. Configure Synapse SQL

Conecte ao workspace Synapse e execute os scripts em ordem:
```sql
-- No Synapse Studio → Develop → SQL script
-- Execute cada arquivo em ordem numérica:
-- 00_setup_database.sql
-- 01_create_external_data_sources.sql
-- 02_create_external_tables.sql
-- 03_create_analytics_views.sql
```

> Substitua `stfinancialdata` pelo nome real da sua conta ADLS nos scripts SQL.

### 9. Abra o dashboard no Power BI Desktop

Abra `powerbi/financial_dashboard.Report/definition.pbir` no Power BI Desktop (Jun 2023+). Configure a conexão com o endpoint Synapse Serverless em **Home → Transform Data → Data Source Settings**.

### 10. Rode os testes localmente (requer Java 11+)

```bash
make test
# Ou: pytest tests/unit/ -v
```

---

## Dataset

| Campo | Descrição |
|-------|-----------|
| `Date` | Data de negociação (yyyy-MM-dd) |
| `Symbol` | Ticker do ativo (AAPL, MSFT, GOOGL, AMZN, TSLA) |
| `Open` | Preço de abertura |
| `High` | Máxima do dia |
| `Low` | Mínima do dia |
| `Close` | Preço de fechamento |
| `Volume` | Volume negociado |
| `OpenInt` | Open Interest (futuros; 0 para ações) |

**Fonte:** [Kaggle — adhoppin/financial-data](https://www.kaggle.com/datasets/adhoppin/financial-data/data)

---

## Métricas derivadas (Silver layer)

| Métrica | Fórmula |
|---------|---------|
| `daily_return_pct` | `(Close - Open) / Open × 100` |
| `intraday_range` | `High - Low` |
| `price_spread_pct` | `(High - Low) / Open × 100` |

## Métricas derivadas (Gold layer)

| Métrica | Descrição |
|---------|-----------|
| `ma_close_7d` | Média móvel simples de 7 dias do Close |
| `ma_close_30d` | Média móvel simples de 30 dias do Close |
| `ma_volume_7d` | Média móvel de 7 dias do Volume |
| `prev_close` | Fechamento do dia anterior (lag 1) |
| `day_over_day_return_pct` | Retorno dia-a-dia: `(Close - prev_close) / prev_close × 100` |
| `cumulative_return_pct` | Retorno acumulado desde o primeiro registro do símbolo |

---

## Contribuindo

1. Crie uma branch: `git checkout -b feat/sua-feature`
2. Siga commits convencionais: `feat:`, `fix:`, `docs:`, `test:`, `chore:`
3. Garanta que `make test` e `make lint` passem
4. Abra um Pull Request — o CI valida lint, testes e terraform automaticamente

---

