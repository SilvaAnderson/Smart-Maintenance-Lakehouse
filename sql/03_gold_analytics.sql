-- Camada Gold (Analytics)
-- Dados prontos para consumo analítico e ML de predição das 5 falhas: TWF, HDF, PWF, OSF, RNF
USE CATALOG hive_metastore;
USE smart_maintenance;
CREATE TABLE IF NOT EXISTS etl_pipeline_control (
    pipeline_name STRING,
    last_silver_commit_version BIGINT,
    last_run_ts TIMESTAMP
) USING DELTA;
MERGE INTO etl_pipeline_control AS tgt USING (
    SELECT 'gold_from_silver_cdf' AS pipeline_name,
        CAST(-1 AS BIGINT) AS last_silver_commit_version,
        current_timestamp() AS last_run_ts
) AS src ON tgt.pipeline_name = src.pipeline_name
WHEN NOT MATCHED THEN
INSERT (
        pipeline_name,
        last_silver_commit_version,
        last_run_ts
    )
VALUES (
        src.pipeline_name,
        src.last_silver_commit_version,
        src.last_run_ts
    );
CREATE OR REPLACE TEMP VIEW vw_silver_cdf_incremental AS
SELECT _commit_version,
    udi,
    product_id,
    machine_type,
    event_date,
    event_hour,
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
WHERE _change_type IN ('insert', 'update_postimage')
    AND _commit_version > (
        SELECT COALESCE(MAX(last_silver_commit_version), -1)
        FROM etl_pipeline_control
        WHERE pipeline_name = 'gold_from_silver_cdf'
    );
CREATE TABLE IF NOT EXISTS gold_ai4i2020_features (
    udi INT,
    product_id STRING,
    machine_type STRING,
    event_date DATE,
    event_hour INT,
    air_temperature_k DOUBLE,
    process_temperature_k DOUBLE,
    rotational_speed_rpm INT,
    torque_nm DOUBLE,
    tool_wear_min INT,
    machine_failure INT,
    twf_label INT,
    hdf_label INT,
    pwf_label INT,
    osf_label INT,
    rnf_label INT,
    _gold_loaded_ts TIMESTAMP
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);
MERGE INTO gold_ai4i2020_features AS tgt USING (
    SELECT udi,
        product_id,
        machine_type,
        event_date,
        event_hour,
        air_temperature_k,
        process_temperature_k,
        rotational_speed_rpm,
        torque_nm,
        tool_wear_min,
        machine_failure,
        twf AS twf_label,
        hdf AS hdf_label,
        pwf AS pwf_label,
        osf AS osf_label,
        rnf AS rnf_label,
        current_timestamp() AS _gold_loaded_ts
    FROM vw_silver_cdf_incremental
) AS src ON tgt.udi = src.udi
WHEN MATCHED THEN
UPDATE
SET product_id = src.product_id,
    machine_type = src.machine_type,
    event_date = src.event_date,
    event_hour = src.event_hour,
    air_temperature_k = src.air_temperature_k,
    process_temperature_k = src.process_temperature_k,
    rotational_speed_rpm = src.rotational_speed_rpm,
    torque_nm = src.torque_nm,
    tool_wear_min = src.tool_wear_min,
    machine_failure = src.machine_failure,
    twf_label = src.twf_label,
    hdf_label = src.hdf_label,
    pwf_label = src.pwf_label,
    osf_label = src.osf_label,
    rnf_label = src.rnf_label,
    _gold_loaded_ts = src._gold_loaded_ts
    WHEN NOT MATCHED THEN
INSERT (
        udi,
        product_id,
        machine_type,
        event_date,
        event_hour,
        air_temperature_k,
        process_temperature_k,
        rotational_speed_rpm,
        torque_nm,
        tool_wear_min,
        machine_failure,
        twf_label,
        hdf_label,
        pwf_label,
        osf_label,
        rnf_label,
        _gold_loaded_ts
    )
VALUES (
        src.udi,
        src.product_id,
        src.machine_type,
        src.event_date,
        src.event_hour,
        src.air_temperature_k,
        src.process_temperature_k,
        src.rotational_speed_rpm,
        src.torque_nm,
        src.tool_wear_min,
        src.machine_failure,
        src.twf_label,
        src.hdf_label,
        src.pwf_label,
        src.osf_label,
        src.rnf_label,
        src._gold_loaded_ts
    );
CREATE TABLE IF NOT EXISTS gold_ai4i2020_daily_agg (
    product_id STRING,
    machine_type STRING,
    event_date DATE,
    total_registros BIGINT,
    avg_air_temperature_k DOUBLE,
    avg_process_temperature_k DOUBLE,
    avg_rotational_speed_rpm DOUBLE,
    avg_torque_nm DOUBLE,
    avg_tool_wear_min DOUBLE,
    total_twf BIGINT,
    total_hdf BIGINT,
    total_pwf BIGINT,
    total_osf BIGINT,
    total_rnf BIGINT,
    _gold_agg_loaded_ts TIMESTAMP
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);
CREATE OR REPLACE TEMP VIEW vw_daily_keys_incremental AS
SELECT DISTINCT product_id,
    machine_type,
    event_date
FROM vw_silver_cdf_incremental;
MERGE INTO gold_ai4i2020_daily_agg AS tgt USING (
    SELECT product_id,
        machine_type,
        event_date,
        COUNT(*) AS total_registros,
        AVG(air_temperature_k) AS avg_air_temperature_k,
        AVG(process_temperature_k) AS avg_process_temperature_k,
        AVG(rotational_speed_rpm) AS avg_rotational_speed_rpm,
        AVG(torque_nm) AS avg_torque_nm,
        AVG(tool_wear_min) AS avg_tool_wear_min,
        SUM(twf_label) AS total_twf,
        SUM(hdf_label) AS total_hdf,
        SUM(pwf_label) AS total_pwf,
        SUM(osf_label) AS total_osf,
        SUM(rnf_label) AS total_rnf,
        current_timestamp() AS _gold_agg_loaded_ts
    FROM gold_ai4i2020_features
    WHERE (
            product_id,
            machine_type,
            event_date
        ) IN (
            SELECT product_id,
                machine_type,
                event_date
            FROM vw_daily_keys_incremental
        )
    GROUP BY product_id,
        machine_type,
        event_date
) AS src ON tgt.product_id = src.product_id
AND tgt.machine_type = src.machine_type
AND tgt.event_date = src.event_date
WHEN MATCHED THEN
UPDATE
SET total_registros = src.total_registros,
    avg_air_temperature_k = src.avg_air_temperature_k,
    avg_process_temperature_k = src.avg_process_temperature_k,
    avg_rotational_speed_rpm = src.avg_rotational_speed_rpm,
    avg_torque_nm = src.avg_torque_nm,
    avg_tool_wear_min = src.avg_tool_wear_min,
    total_twf = src.total_twf,
    total_hdf = src.total_hdf,
    total_pwf = src.total_pwf,
    total_osf = src.total_osf,
    total_rnf = src.total_rnf,
    _gold_agg_loaded_ts = src._gold_agg_loaded_ts
    WHEN NOT MATCHED THEN
INSERT (
        product_id,
        machine_type,
        event_date,
        total_registros,
        avg_air_temperature_k,
        avg_process_temperature_k,
        avg_rotational_speed_rpm,
        avg_torque_nm,
        avg_tool_wear_min,
        total_twf,
        total_hdf,
        total_pwf,
        total_osf,
        total_rnf,
        _gold_agg_loaded_ts
    )
VALUES (
        src.product_id,
        src.machine_type,
        src.event_date,
        src.total_registros,
        src.avg_air_temperature_k,
        src.avg_process_temperature_k,
        src.avg_rotational_speed_rpm,
        src.avg_torque_nm,
        src.avg_tool_wear_min,
        src.total_twf,
        src.total_hdf,
        src.total_pwf,
        src.total_osf,
        src.total_rnf,
        src._gold_agg_loaded_ts
    );
-- Tabela para auditoria de predição de falhas (integração futura com modelo)
CREATE TABLE IF NOT EXISTS gold_ai4i2020_failure_predictions (
    udi INT,
    product_id STRING,
    prediction_ts TIMESTAMP,
    pred_twf INT,
    pred_hdf INT,
    pred_pwf INT,
    pred_osf INT,
    pred_rnf INT,
    model_name STRING,
    model_version STRING,
    prediction_score DOUBLE,
    _pred_loaded_ts TIMESTAMP
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);
-- Baseline heurístico apenas para inicialização da arquitetura.
-- Substituir por output real de modelo de ML.
MERGE INTO gold_ai4i2020_failure_predictions AS tgt USING (
    SELECT udi,
        product_id,
        current_timestamp() AS prediction_ts,
        CASE
            WHEN tool_wear_min > 200 THEN 1
            ELSE 0
        END AS pred_twf,
        CASE
            WHEN process_temperature_k - air_temperature_k > 10 THEN 1
            ELSE 0
        END AS pred_hdf,
        CASE
            WHEN torque_nm > 55 THEN 1
            ELSE 0
        END AS pred_pwf,
        CASE
            WHEN rotational_speed_rpm < 1200
            AND torque_nm > 50 THEN 1
            ELSE 0
        END AS pred_osf,
        CASE
            WHEN machine_type = 'L'
            AND tool_wear_min > 180 THEN 1
            ELSE 0
        END AS pred_rnf,
        'baseline_rule_engine' AS model_name,
        'v0' AS model_version,
        0.5 AS prediction_score,
        current_timestamp() AS _pred_loaded_ts
    FROM gold_ai4i2020_features
    WHERE udi IN (
            SELECT DISTINCT udi
            FROM vw_silver_cdf_incremental
        )
) AS src ON tgt.udi = src.udi
AND tgt.model_name = src.model_name
AND tgt.model_version = src.model_version
WHEN MATCHED THEN
UPDATE
SET product_id = src.product_id,
    prediction_ts = src.prediction_ts,
    pred_twf = src.pred_twf,
    pred_hdf = src.pred_hdf,
    pred_pwf = src.pred_pwf,
    pred_osf = src.pred_osf,
    pred_rnf = src.pred_rnf,
    prediction_score = src.prediction_score,
    _pred_loaded_ts = src._pred_loaded_ts
    WHEN NOT MATCHED THEN
INSERT (
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
        prediction_score,
        _pred_loaded_ts
    )
VALUES (
        src.udi,
        src.product_id,
        src.prediction_ts,
        src.pred_twf,
        src.pred_hdf,
        src.pred_pwf,
        src.pred_osf,
        src.pred_rnf,
        src.model_name,
        src.model_version,
        src.prediction_score,
        src._pred_loaded_ts
    );
UPDATE etl_pipeline_control
SET last_silver_commit_version = COALESCE(
        (
            SELECT MAX(_commit_version)
            FROM vw_silver_cdf_incremental
        ),
        last_silver_commit_version
    ),
    last_run_ts = current_timestamp()
WHERE pipeline_name = 'gold_from_silver_cdf';
-- Otimização Delta embutida na transformação Gold
OPTIMIZE gold_ai4i2020_features ZORDER BY (product_id, event_date);
OPTIMIZE gold_ai4i2020_failure_predictions ZORDER BY (product_id, prediction_ts);