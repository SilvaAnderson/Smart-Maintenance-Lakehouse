-- Change Data Feed (CDF)
-- Auditoria de mudanças de condições das máquinas entre Silver e Gold
USE CATALOG hive_metastore;
USE smart_maintenance;
-- Histórico de mudanças na Silver
SELECT _change_type,
    _commit_version,
    _commit_timestamp,
    udi,
    product_id,
    machine_type,
    air_temperature_k,
    process_temperature_k,
    rotational_speed_rpm,
    torque_nm,
    tool_wear_min,
    machine_failure,
    twf,
    hdf,
    pwf,
    osf,
    rnf
FROM table_changes('silver_ai4i2020_validated', 0)
ORDER BY _commit_version,
    udi;
-- Histórico de mudanças na Gold de predições
SELECT _change_type,
    _commit_version,
    _commit_timestamp,
    udi,
    product_id,
    prediction_ts,
    pred_twf,
    pred_hdf,
    pred_pwf,
    pred_osf,
    pred_rnf,
    model_name,
    model_version,
    prediction_score
FROM table_changes('gold_ai4i2020_failure_predictions', 0)
ORDER BY _commit_version,
    udi;
-- Auditoria: quando uma condição Silver resultou em predição de falha na Gold
WITH silver_changes AS (
    SELECT _commit_version AS silver_version,
        _commit_timestamp AS silver_commit_ts,
        udi,
        product_id,
        air_temperature_k,
        process_temperature_k,
        rotational_speed_rpm,
        torque_nm,
        tool_wear_min
    FROM table_changes('silver_ai4i2020_validated', 0)
    WHERE _change_type IN ('insert', 'update_postimage')
),
gold_pred_changes AS (
    SELECT _commit_version AS gold_version,
        _commit_timestamp AS gold_commit_ts,
        udi,
        product_id,
        pred_twf,
        pred_hdf,
        pred_pwf,
        pred_osf,
        pred_rnf,
        model_name,
        model_version
    FROM table_changes('gold_ai4i2020_failure_predictions', 0)
    WHERE _change_type IN ('insert', 'update_postimage')
)
SELECT s.silver_version,
    s.silver_commit_ts,
    g.gold_version,
    g.gold_commit_ts,
    s.udi,
    s.product_id,
    s.air_temperature_k,
    s.process_temperature_k,
    s.rotational_speed_rpm,
    s.torque_nm,
    s.tool_wear_min,
    g.pred_twf,
    g.pred_hdf,
    g.pred_pwf,
    g.pred_osf,
    g.pred_rnf,
    g.model_name,
    g.model_version
FROM silver_changes s
    INNER JOIN gold_pred_changes g ON s.udi = g.udi
    AND s.product_id = g.product_id
ORDER BY g.gold_commit_ts DESC;