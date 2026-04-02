# Arquitetura Medallion - Smart Maintenance

## Fluxo

1. **Bronze (Raw)**
   - Entrada: `ai4i2020.xls`.
   - Conversão para Delta.
   - Estratégia append-only para manter histórico completo.
   - CDF habilitado para rastreabilidade de carga.

2. **Silver (Validated)**
   - Casting tipado (schema enforcement via tabela Delta tipada).
   - Filtro de campos obrigatórios e trilha de quarentena.
   - `Generated Columns`:
     - `event_date = CAST(log_ts AS DATE)`
     - `event_hour = HOUR(log_ts)`

3. **Gold (Analytics)**
   - `gold_ai4i2020_features`: dataset pronto para treino/inferência de modelo.
   - `gold_ai4i2020_daily_agg`: agregações por produto/data para análise histórica.
   - `gold_ai4i2020_failure_predictions`: predições para TWF/HDF/PWF/OSF/RNF.

4. **Visualization (Decoupled Serving)**
   - `src/streamlit_dashboard.py` conecta via SQL Warehouse usando `databricks-sql-python`.
   - Filtros dinâmicos por `product_id` e `machine_type`.
   - Widgets analíticos:
     - gauge de risco por desgaste da ferramenta;
     - heatmap `Air temperature x Torque`;
     - gráfico de barras da distribuição de falhas.

## Governança e Auditoria

- **Change Data Feed (CDF)** em Silver e Gold.
- Auditoria com `table_changes(...)` para rastrear:
  - Mudanças no estado dos sensores.
  - Momento de geração/atualização das predições de falha.

## Performance

- `OPTIMIZE ... ZORDER BY (product_id, log_ts/prediction_ts)` para reduzir scanning.

## Serving desacoplado

- O consumo analítico do dashboard ocorre pelo SQL Warehouse do Databricks, sem dependência direta do cluster ETL.
- Essa abordagem separa computação de transformação e camada de visualização, reduzindo acoplamento operacional.

## Time Travel

- `VERSION AS OF` e `TIMESTAMP AS OF` para recuperar:
  - Estado dos sensores antes de uma predição de falha.
  - Estado histórico em versões específicas para auditoria.