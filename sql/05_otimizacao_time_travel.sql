-- Otimização de performance e Time Travel
USE CATALOG hive_metastore;
USE smart_maintenance;
-- Z-Ordering para acelerar análise histórica por Product ID e tempo
OPTIMIZE silver_ai4i2020_validated ZORDER BY (product_id, log_ts);
OPTIMIZE gold_ai4i2020_features ZORDER BY (product_id, event_date);
OPTIMIZE gold_ai4i2020_failure_predictions ZORDER BY (product_id, prediction_ts);
-- Alternativa em ambientes com Liquid Clustering habilitado:
-- ALTER TABLE gold_ai4i2020_features CLUSTER BY (product_id, event_date);
-- Time Travel: visualizar histórico de versões
DESCRIBE HISTORY silver_ai4i2020_validated;
DESCRIBE HISTORY gold_ai4i2020_failure_predictions;
-- Exemplo 1: estado da Silver na versão anterior
SELECT *
FROM silver_ai4i2020_validated VERSION AS OF 0
WHERE machine_failure = 1;
-- Exemplo 2: estado da Gold por timestamp (ajuste timestamp conforme histórico real)
SELECT *
FROM gold_ai4i2020_failure_predictions TIMESTAMP AS OF '2026-04-02T00:00:00Z'
WHERE pred_twf = 1
    OR pred_hdf = 1
    OR pred_pwf = 1
    OR pred_osf = 1
    OR pred_rnf = 1;
-- Exemplo 3: estado imediatamente antes de uma falha prevista
-- 1) Descobrir o instante da predição de falha
WITH first_failure_pred AS (
    SELECT min(prediction_ts) AS first_pred_ts
    FROM gold_ai4i2020_failure_predictions
    WHERE pred_twf = 1
        OR pred_hdf = 1
        OR pred_pwf = 1
        OR pred_osf = 1
        OR pred_rnf = 1
)
SELECT *
FROM silver_ai4i2020_validated TIMESTAMP AS OF (
        SELECT first_pred_ts
        FROM first_failure_pred
    )
WHERE product_id IS NOT NULL;