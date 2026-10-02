<div align="center">

<h1>Financial Data — End-to-End Platform</h1>

<p>
  <a href="https://github.com/fabianoamaralbr/data-end-to-end/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/fabianoamaralbr/data-end-to-end/actions/workflows/ci.yml/badge.svg?branch=main"/></a>
  <img alt="Python 3.11" src="https://img.shields.io/badge/python-3.11-blue?style=flat-square&color=1f4fa0"/>
  <img alt="PySpark 3.5" src="https://img.shields.io/badge/PySpark-3.5-orange?style=flat-square"/>
  <img alt="Delta Lake 3.1" src="https://img.shields.io/badge/Delta_Lake-3.1-blue?style=flat-square&color=002056"/>
  <img alt="Terraform" src="https://img.shields.io/badge/IaC-Terraform-purple?style=flat-square"/>
  <a href="LICENSE"><img alt="License MIT" src="https://img.shields.io/badge/license-MIT-gold?style=flat-square&color=cdac80"/></a>
</p>

</div>

---

## Problema

Dados financeiros públicos de mercado de ações (OHLCV — Open, High, Low, Close, Volume) são valiosos para análises de risco, retorno e tendência, mas chegam em formato CSV bruto, sem qualidade garantida, sem histórico de transformação e sem governança. Este projeto resolve esse problema construindo uma **plataforma de dados end-to-end** no Azure que:

1. **Ingere** via Azure Data Factory: carga histórica do Kaggle (OHLCV) e carga **incremental diária** de séries do Banco Central (SGS — Selic, CDI, PTAX)
2. **Transforma** em uma arquitetura medallion (bronze → silver → gold) via Databricks + Unity Catalog, de forma **incremental e idempotente**, com **data contracts** e **quarentena** de registros inválidos
3. **Expõe** via Synapse Analytics Serverless SQL com tabelas externas Delta Lake
4. **Visualiza** em Power BI com métricas de retorno, médias móveis e sinais de tendência

Fontes: o dataset [`adhoppin/financial-data`](https://www.kaggle.com/datasets/adhoppin/financial-data/data) do Kaggle (OHLCV histórico de AAPL, MSFT, GOOGL, AMZN, TSLA — estático, usado como backfill) e a [API de dados abertos do Banco Central](https://dadosabertos.bcb.gov.br/) (SGS — atualizada diariamente, é a fonte que exercita a carga incremental).

---

## Arquitetura

```
Kaggle (backfill, ZIP→CSV)      Banco Central SGS (diário, JSON — janela móvel de 7 dias)
        │  ADF Copy                     │  ADF Copy (tumbling window trigger)
        ▼                               ▼
ADLS Gen2 raw/ ── financial-data/*.csv · bcb_sgs/series_code=<n>/<janela>.json
        │
        │  file arrival trigger → Databricks Workflow (Unity Catalog, managed identity)
        │
        ├─► bronze.ohlcv · bronze.bcb_sgs        Auto Loader + MERGE insert-only por _ingestion_key
        │                                        (reexecutar não duplica)
        │
        ├─► silver.ohlcv · silver.bcb_sgs        MERGE por chave de negócio, versão mais recente vence
        │   silver.*_quarantine                  registros inválidos + motivo da rejeição
        │   ops.dq_metrics                       métricas por batch + circuit breaker
        │
        └─► gold.ohlcv_detail                    Change Data Feed → recalcula só os símbolos afetados
            gold.ohlcv_daily_summary             (MA7, MA30, retornos)
              │
              │  Synapse Serverless SQL (External Tables, Delta)
              │
              └─► gold.vw_rolling_volatility · gold.vw_symbol_performance
                  ops.vw_dq_daily · ops.vw_quarantine_reasons
                        │
                        │  Power BI DirectQuery
                        │
                        └─► financial_dashboard.pbip
```

> Diagrama completo com Mermaid, Sequence Diagram e ADRs: [`architecture.md`](architecture.md)

### Stack

| Camada | Tecnologia |
|--------|-----------|
| Ingestão | Azure Data Factory (HTTP → ADLS, Managed Identity, tumbling window) |
| Armazenamento | Azure Data Lake Storage Gen2 (raw/bronze/silver/gold + catálogo UC) |
| Governança | Unity Catalog (access connector com managed identity, external locations) |
| Transformação | Azure Databricks (Auto Loader, Delta MERGE, Change Data Feed) — lógica no pacote `src/transforms` |
| Analytics SQL | Azure Synapse Analytics Serverless SQL (external tables, Delta format) |
| Visualização | Power BI (PBIP format, DirectQuery, semantic model com DAX measures) |
| IaC | Terraform (storage, adf, databricks, unity_catalog, synapse; state remoto no Azure) |
| CI | GitHub Actions: lint, testes unitários + integração Delta com cobertura de `src/transforms`, terraform validate; plan comentado no PR e apply com aprovação |

---

## Estrutura do Repositório

```
data-end-to-end/
├── .github/workflows/
│   ├── ci.yml                        # lint + testes (cobertura de src/transforms) + terraform validate
│   └── terraform.yml                 # plan comentado no PR; apply em main com aprovação
├── src/transforms/                   # TODA a lógica do pipeline (importada por notebooks e testes)
│   ├── contracts.py                  # data contracts (StructType) → DDL + validação
│   ├── bronze.py                     # metadados + chave de ingestão
│   ├── silver.py · sgs.py            # tipagem, quarentena, dedup determinística (OHLCV, BCB)
│   ├── gold.py                       # MA7, MA30, retornos
│   ├── quality.py                    # regras, métricas por batch, circuit breaker
│   ├── delta_ops.py                  # MERGEs idempotentes
│   ├── batch.py                      # o que cada foreachBatch executa
│   └── datasets.py · tables.py       # registro de datasets/tabelas, DDL e OPTIMIZE
├── databricks/
│   ├── notebooks/                    # finos: montam o stream e chamam src/transforms
│   │   ├── 00_setup_unity_catalog.py # cria/valida tabelas a partir dos contratos
│   │   ├── 01_raw_to_bronze.py       # Auto Loader → bronze (dataset=ohlcv|bcb_sgs)
│   │   ├── 02_bronze_to_silver.py    # bronze → silver + quarentena + métricas
│   │   └── 03_silver_to_gold.py      # CDF da silver → gold por símbolo afetado
│   └── jobs/medallion_workflow.json  # workflow com file arrival trigger
├── adf/                              # linked services, datasets, pipelines e trigger
├── synapse/sql/                      # external tables (espelham os contratos) e views
├── powerbi/                          # PBIP/TMDL (cache e settings locais fora do git)
├── infra/                            # Terraform (root + 5 módulos, backend remoto parcial)
├── tests/
│   ├── unit/                         # transformações, contratos, regras (sem escrita)
│   └── integration/                  # tabelas Delta reais: idempotência, MERGE, quarentena
├── data/sample/financial_sample.csv
└── scripts/                          # download Kaggle, bootstrap do tfstate, amostra
```

---

## Decisões Técnicas

### Por que Delta Lake em todas as camadas?

Delta Lake oferece ACID transactions, schema evolution e time-travel. No Synapse Serverless SQL, basta declarar `FILE_FORMAT = DELTA` numa external table — o engine lê o transaction log e enxerga sempre o snapshot mais recente. Não há ETL adicional entre Databricks e Synapse.

### Por que Synapse Serverless (não Dedicated Pool)?

Zero custo em idle. Cobrança por TB escaneado. Para um dataset financeiro com ~500K linhas (escala do Kaggle), o custo mensal é de centavos. As tabelas são gravadas sem deletion vectors, porque o Serverless SQL só lê Delta reader v1.

### Por que PBIP (não .pbix)?

`.pbix` é binário — não é diffável em git. O formato `.pbip` (Power BI Project) armazena o semantic model em TMDL (`definition/tables/*.tmdl`, um arquivo por tabela) e o layout em `report.json`. Cache (`cache.abf`) e configurações locais (`localSettings.json`) ficam fora do git, como orienta a documentação do PBIP. Toda mudança de medida DAX, cor ou visual aparece num diff legível no PR.

### Como o pipeline garante idempotência e incrementalidade?

- **Bronze:** o Auto Loader só lê arquivos novos (checkpoint) e cada registro recebe uma `_ingestion_key` (hash de arquivo + conteúdo). A escrita é um `MERGE` insert-only por essa chave: reexecutar o job, ou perder o checkpoint, não duplica nada.
- **Silver:** lê apenas os registros novos da bronze e faz `MERGE` por chave de negócio (`Symbol, Date` / `series_code, ref_date`). A deduplicação usa uma window ordenada por `_ingested_at, _source_file, _ingestion_key` (não `dropDuplicates`), e o mesmo critério decide no `MERGE` se a linha existente é substituída: uma versão antiga que chega atrasada não sobrescreve a nova.
- **Gold:** lê o Change Data Feed da silver e recalcula apenas os símbolos afetados, substituindo a fatia deles com `MERGE ... WHEN NOT MATCHED BY SOURCE DELETE`.

### Como os data contracts são aplicados?

Os schemas de todas as tabelas vivem em `src/transforms/contracts.py`. O DDL é gerado a partir deles, todo DataFrame é validado antes da escrita e nenhuma escrita usa `mergeSchema`/`overwriteSchema`. Uma coluna nova na origem cai em `_rescued_data` e o registro vai para a quarentena com o motivo `schema_drift`; uma tabela cujo schema divirja do contrato faz o setup falhar com `SchemaContractError`.

### O que acontece com registros inválidos?

Vão para `silver.*_quarantine` com os valores originais e o motivo (`close_invalid;volume_invalid`, por exemplo). Cada batch grava `rows_in / rows_valid / rows_quarantined / rows_deduplicated` em `ops.dq_metrics`. Se a taxa de quarentena passar de `max_quarantine_rate` (5% por padrão), o job falha **antes** de tocar a silver; quarentena e métricas já ficam gravadas para investigação.

### Por que não particionar por data?

OHLCV diário particionado por `Date` gera milhares de partições minúsculas (o problema de small files). As tabelas não são particionadas; o layout é otimizado com `OPTIMIZE ... ZORDER BY (Symbol, Date)`. Liquid clustering foi descartado porque o Synapse Serverless precisa ler as tabelas.

> Todos os ADRs completos em [`architecture.md`](architecture.md)

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
git clone https://github.com/fabianoamaralbr/data-end-to-end.git
cd data-end-to-end
cp .env.example .env
# Edite .env com suas credenciais Azure e Kaggle
```

### 2. Instale dependências Python

```bash
make install   # requirements-dev + pacote src/transforms (pip install -e .)
```

### 3. Provisione a infraestrutura Azure

```bash
az login
./scripts/bootstrap_tfstate.sh                            # uma vez: storage do state remoto
cp infra/backend.hcl.example infra/backend.hcl
cp infra/terraform.tfvars.example infra/terraform.tfvars  # senha do Synapse
make tf-plan                                              # gera infra/tfplan — revise
make tf-apply                                             # aplica exatamente o plano revisado
```

No fluxo normal, a infra muda por PR: o workflow `terraform.yml` comenta o plan no PR e o apply roda após o merge, com aprovação do environment `production`. O workspace Databricks precisa estar associado a um metastore Unity Catalog (padrão em workspaces novos).

Os outputs do Terraform fornecem os valores para preencher o `.env` (`ADLS_ACCOUNT_NAME`, `DATABRICKS_HOST`, `UC_CATALOG`, `SYNAPSE_SQL_ENDPOINT`).

### 4. Carga histórica do Kaggle

Caminho de produção: o pipeline `pl_ingest_financial_data` chama a API do Kaggle (`datasets/download/<owner>/<dataset>`, Basic auth com usuário e chave guardados no Key Vault), descompacta o ZIP em trânsito e grava os CSVs em `raw/financial-data/`. Alternativa local para desenvolvimento:

```bash
# Certifique-se de ter KAGGLE_USERNAME e KAGGLE_KEY no .env
make download-data
```

### 5. Publique os artefatos ADF

```bash
make deploy-adf
```

### 6. Crie o job no Databricks

O job lê os notebooks direto do GitHub (`git_source`) e é disparado pela chegada de arquivos no container `raw` (file arrival trigger). O ADF não chama o Databricks.

```bash
databricks auth login --host $DATABRICKS_HOST
make deploy-job        # preenche catálogo/storage/e-mail no JSON e cria o job
```

### 7. Execute o pipeline pela primeira vez

```bash
make run-pipeline
# Ou ative o trigger tr_bcb_sgs_daily no ADF: janelas diárias, com backfill desde o startTime
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
make install   # dependências + pacote transforms em modo editável
make test      # unitários + integração Delta, com cobertura de src/transforms
```

Os testes importam as mesmas funções que os notebooks executam. Os de integração (`@pytest.mark.delta`) gravam tabelas Delta reais e verificam, por exemplo, que reprocessar um batch não altera o resultado. No Windows eles exigem `winutils` e são pulados localmente; no CI rodam sempre.

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

### Banco Central (SGS)

| Código | `series_name` | Descrição |
|-------:|---------------|-----------|
| 1 | `usd_brl_ptax_venda` | Dólar americano — PTAX venda (R$) |
| 11 | `selic_diaria` | Taxa Selic diária (% a.d.) |
| 12 | `cdi_diario` | Taxa CDI diária (% a.d.) |
| 432 | `selic_meta` | Meta Selic definida pelo Copom (% a.a.) |

**Fonte:** [API SGS do Banco Central](https://dadosabertos.bcb.gov.br/), em `silver.bcb_sgs` (`series_code`, `ref_date`, `value`)

---

## Métricas derivadas (Silver layer)

| Métrica | Fórmula |
|---------|---------|
| `intraday_return_pct` | `(Close - Open) / Open × 100` — variação **dentro** do pregão (o retorno entre pregões é `day_over_day_return_pct`, na gold) |
| `intraday_range` | `High - Low` |
| `price_spread_pct` | `(High - Low) / Open × 100` |

## Métricas derivadas (Gold layer)

| Métrica | Descrição |
|---------|-----------|
| `ma_close_7d` | Média móvel simples dos últimos 7 pregões do Close (`NULL` enquanto houver menos de 7) |
| `ma_close_30d` | Média móvel simples dos últimos 30 pregões do Close (`NULL` enquanto houver menos de 30) |
| `ma_volume_7d` | Média móvel dos últimos 7 pregões do Volume (`NULL` enquanto houver menos de 7) |
| `prev_close` | Fechamento do dia anterior (lag 1) |
| `day_over_day_return_pct` | Retorno dia-a-dia: `(Close - prev_close) / prev_close × 100` |
| `cumulative_return_pct` | Retorno acumulado desde o primeiro registro do símbolo |

## Regras de qualidade (Silver)

| Motivo | Regra |
|--------|-------|
| `schema_drift` | Coluna inesperada na origem (`_rescued_data` preenchido) |
| `date_invalid` | Data nula ou fora do formato `yyyy-MM-dd` (OHLCV) / `dd/MM/yyyy` (SGS) |
| `symbol_missing` | Símbolo nulo ou vazio |
| `open_invalid` · `close_invalid` · `high_invalid` · `low_invalid` | Preço nulo, não numérico ou ≤ 0 |
| `high_below_low` | Máxima menor que a mínima |
| `ohlc_inconsistent` | Máxima abaixo de `max(Open, Close)` ou mínima acima de `min(Open, Close)` |
| `volume_invalid` | Volume nulo, não numérico ou negativo |
| `series_unknown` · `value_invalid` | Série SGS fora do catálogo / valor não numérico |

---

## Status do projeto

O que está **verificado automaticamente** a cada PR (workflow `CI`):

- `ruff` em `src/`, `tests/`, `scripts/` e notebooks;
- testes unitários das transformações e testes de integração que gravam tabelas Delta reais (idempotência do MERGE, versão mais recente vence, quarentena, circuit breaker, recálculo incremental da gold), com cobertura mínima de 90% sobre `src/transforms`;
- `terraform fmt -check` e `terraform validate` (sem backend).

O que **ainda não** foi exercitado e é a próxima etapa:

- `terraform plan/apply` e execução ponta a ponta numa assinatura Azure — o workflow `Terraform` fica como *skipped* até os secrets serem configurados;
- o relatório Power BI: o modelo semântico (TMDL, medidas DAX, DirectQuery no Synapse) está versionado, mas `report.json` ainda não tem páginas — os prints entram aqui quando o relatório for montado;
- o schema real do dataset do Kaggle: `data/sample/financial_sample.csv` é **sintético** (`scripts/_gen_sample.py`) e segue o contrato `RAW_OHLCV`; se o arquivo real divergir, os registros caem na quarentena com `schema_drift` e o circuit breaker interrompe a carga — que é exatamente o comportamento desejado.

---

## Contribuindo

1. Crie uma branch: `git checkout -b feat/sua-feature`
2. Siga commits convencionais: `feat:`, `fix:`, `docs:`, `test:`, `chore:`
3. Garanta que `make test` e `make lint` passem
4. Abra um Pull Request — o CI valida lint, testes e terraform automaticamente

---

Licença [MIT](LICENSE) · Fabiano Amaral
