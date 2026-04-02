# Smart Maintenance Lakehouse (ai4i2020)

Projeto de arquitetura de dados com **Medallion Architecture** usando **Delta Lake** para o dataset `ai4i2020`.

## Objetivo

Construir as camadas:
- **Bronze (Raw):** ingestão bruta com histórico completo.
- **Silver (Validated):** limpeza, schema enforcement e generated columns.
- **Gold (Analytics):** tabelas agregadas e feature store para ML (TWF, HDF, PWF, OSF, RNF).
- **Carga incremental:** Silver e Gold com `MERGE` (sem `full overwrite`).

Além disso:
- **Change Data Feed (CDF)** para auditoria de mudanças entre Silver e Gold.
- **Incremental por CDF:** Gold processa apenas novos commits da Silver via `table_changes` + watermark.
- **Otimização** com `OPTIMIZE ... ZORDER BY`.
- **Time Travel** para auditoria de estado antes de falhas.

## Estrutura

- `src/01_ingest_bronze.py` → ingestão do `ai4i2020.xls` para Bronze Delta.
- `src/run_medallion_pipeline.py` → execução única Bronze → Silver → Gold → CDF → otimização.
- `sql/02_silver_validated.sql` → transformação Silver + schema enforcement + generated columns.
- `sql/02a_silver_data_quality.sql` → data quality da Silver (expectations estilo regras de negócio + auditoria).
- `sql/03_gold_analytics.sql` → tabelas Gold para consumo analítico e ML.
- `sql/04_cdf_auditoria.sql` → queries de auditoria com CDF.
- `sql/05_otimizacao_time_travel.sql` → performance e time travel.
- `src/data_quality.py` → regras de qualidade reutilizáveis para validação local.
- `tests/test_data_quality.py` → testes unitários de qualidade dos dados.
- `azure-pipelines.yml` → exemplo de CI/CD no Azure DevOps para testes e deploy no Databricks.
- `docs/architecture.md` → visão arquitetural e fluxo.

## Pré-requisitos

- Databricks Runtime com Delta Lake.
- Arquivo de origem disponível em DBFS/Volume (exemplo: `/dbfs/FileStore/ai4i2020.xls`).
- Biblioteca Python para Excel: `openpyxl`.

### Setup local (opcional)

Para remover alertas de import no editor local:

`pip install -r requirements.txt`

## Execução sugerida (ordem)

1. Executar `src/01_ingest_bronze.py`.
2. Executar `sql/02_silver_validated.sql`.
3. Executar `sql/02a_silver_data_quality.sql`.
4. Executar `sql/03_gold_analytics.sql`.
5. Executar `sql/04_cdf_auditoria.sql`.
6. Executar `sql/05_otimizacao_time_travel.sql`.

## Qualidade de dados e testes

- A Silver gera auditoria de qualidade em `silver_ai4i2020_quality_audit` via `sql/02a_silver_data_quality.sql`.
- Regras cobertas: nulidade de chaves, faixa de temperatura, rotação positiva, torque e desgaste não negativos.
- Testes unitários locais:

`pytest -q`

## CI/CD (Azure DevOps)

- Pipeline em `azure-pipelines.yml` com 2 estágios:
	- `Test`: instala dependências e executa `pytest`.
	- `DeployDatabricks`: publica o projeto no workspace Databricks com `databricks-cli`.
- Configurar variáveis secretas no Azure DevOps:
	- `DATABRICKS_HOST`
	- `DATABRICKS_TOKEN`

## Execução única (recomendado para Job)

Você pode executar tudo em sequência usando:

`src/run_medallion_pipeline.py`

Variáveis de ambiente suportadas:
- `AI4I_SOURCE_FILE` (default: `/dbfs/FileStore/ai4i2020.xls`)
- `AI4I_TARGET_CATALOG` (default: `hive_metastore`)
- `AI4I_TARGET_SCHEMA` (default: `smart_maintenance`)
- `AI4I_SQL_DIR` (default: pasta `sql` do próprio projeto)

Exemplo:

PowerShell:

`$env:AI4I_SOURCE_FILE='/dbfs/FileStore/ai4i2020.xls'`

`$env:AI4I_TARGET_CATALOG='hive_metastore'`

`$env:AI4I_TARGET_SCHEMA='smart_maintenance'`

`python .\src\run_medallion_pipeline.py`

## Resultado esperado

- Tabela Bronze com histórico completo de ingestão.
- Tabela Silver tipada, validada e atualizada incrementalmente por chave (`udi`).
- Evidências de qualidade da Silver persistidas em tabela de auditoria + testes unitários versionados.
- Tabelas Gold prontas para treinamento/inferência, atualizadas incrementalmente com `MERGE` e CDF da Silver.
- Controle de progresso incremental em `etl_pipeline_control` (último `_commit_version` processado).
- Auditoria com CDF e consultas históricas com Time Travel.
- Otimização Delta aplicada durante as transformações com `OPTIMIZE ... ZORDER BY`.
