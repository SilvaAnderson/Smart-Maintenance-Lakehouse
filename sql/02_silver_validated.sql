-- Camada Silver (Validated)
-- Requisitos atendidos:
-- 1) limpeza e transformação
-- 2) schema enforcement
-- 3) generated columns de data/hora a partir de log_ts
USE CATALOG hive_metastore;
CREATE SCHEMA IF NOT EXISTS smart_maintenance;
USE smart_maintenance;
CREATE TABLE IF NOT EXISTS silver_ai4i2020_validated (
    udi INT,
    product_id STRING,
    machine_type STRING,
    air_temperature_k DOUBLE,
    process_temperature_k DOUBLE,
    rotational_speed_rpm INT,
    torque_nm DOUBLE,
    tool_wear_min INT,
    machine_failure INT,
    twf INT,
    hdf INT,
    pwf INT,
    osf INT,
    rnf INT,
    ingestion_ts TIMESTAMP,
    source_file STRING,
    log_ts TIMESTAMP,
    event_date DATE GENERATED ALWAYS AS (CAST(log_ts AS DATE)),
    event_hour INT GENERATED ALWAYS AS (HOUR(log_ts)),
    _silver_loaded_ts TIMESTAMP
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);
CREATE TABLE IF NOT EXISTS silver_ai4i2020_quarantine (
    raw_payload STRING,
    error_reason STRING,
    _quarantine_ts TIMESTAMP
) USING DELTA;
MERGE INTO silver_ai4i2020_validated AS tgt USING (
    SELECT CAST(udi AS INT) AS udi,
        CAST(product_id AS STRING) AS product_id,
        CAST(machine_type AS STRING) AS machine_type,
        CAST(air_temperature_k AS DOUBLE) AS air_temperature_k,
        CAST(process_temperature_k AS DOUBLE) AS process_temperature_k,
        CAST(rotational_speed_rpm AS INT) AS rotational_speed_rpm,
        CAST(torque_nm AS DOUBLE) AS torque_nm,
        CAST(tool_wear_min AS INT) AS tool_wear_min,
        CAST(machine_failure AS INT) AS machine_failure,
        CAST(twf AS INT) AS twf,
        CAST(hdf AS INT) AS hdf,
        CAST(pwf AS INT) AS pwf,
        CAST(osf AS INT) AS osf,
        CAST(rnf AS INT) AS rnf,
        CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,
        CAST(source_file AS STRING) AS source_file,
        CAST(log_ts AS TIMESTAMP) AS log_ts,
        current_timestamp() AS _silver_loaded_ts
    FROM bronze_ai4i2020_raw
    WHERE product_id IS NOT NULL
        AND machine_type IS NOT NULL
        AND log_ts IS NOT NULL
) AS src ON tgt.udi = src.udi
WHEN MATCHED THEN
UPDATE
SET product_id = src.product_id,
    machine_type = src.machine_type,
    air_temperature_k = src.air_temperature_k,
    process_temperature_k = src.process_temperature_k,
    rotational_speed_rpm = src.rotational_speed_rpm,
    torque_nm = src.torque_nm,
    tool_wear_min = src.tool_wear_min,
    machine_failure = src.machine_failure,
    twf = src.twf,
    hdf = src.hdf,
    pwf = src.pwf,
    osf = src.osf,
    rnf = src.rnf,
    ingestion_ts = src.ingestion_ts,
    source_file = src.source_file,
    log_ts = src.log_ts,
    _silver_loaded_ts = src._silver_loaded_ts
    WHEN NOT MATCHED THEN
INSERT (
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
        rnf,
        ingestion_ts,
        source_file,
        log_ts,
        _silver_loaded_ts
    )
VALUES (
        src.udi,
        src.product_id,
        src.machine_type,
        src.air_temperature_k,
        src.process_temperature_k,
        src.rotational_speed_rpm,
        src.torque_nm,
        src.tool_wear_min,
        src.machine_failure,
        src.twf,
        src.hdf,
        src.pwf,
        src.osf,
        src.rnf,
        src.ingestion_ts,
        src.source_file,
        src.log_ts,
        src._silver_loaded_ts
    );
-- Exemplo de registros inválidos para trilha de qualidade
MERGE INTO silver_ai4i2020_quarantine AS tgt USING (
    SELECT to_json(
            named_struct(
                'udi',
                udi,
                'product_id',
                product_id,
                'machine_type',
                machine_type,
                'air_temperature_k',
                air_temperature_k,
                'process_temperature_k',
                process_temperature_k,
                'rotational_speed_rpm',
                rotational_speed_rpm,
                'torque_nm',
                torque_nm,
                'tool_wear_min',
                tool_wear_min,
                'machine_failure',
                machine_failure,
                'twf',
                twf,
                'hdf',
                hdf,
                'pwf',
                pwf,
                'osf',
                osf,
                'rnf',
                rnf,
                'ingestion_ts',
                ingestion_ts,
                'source_file',
                source_file,
                'log_ts',
                log_ts
            )
        ) AS raw_payload,
        'Campos obrigatórios ausentes (product_id/machine_type/log_ts)' AS error_reason,
        current_timestamp() AS _quarantine_ts
    FROM bronze_ai4i2020_raw
    WHERE product_id IS NULL
        OR machine_type IS NULL
        OR log_ts IS NULL
) AS src ON tgt.raw_payload = src.raw_payload
AND tgt.error_reason = src.error_reason
WHEN MATCHED THEN
UPDATE
SET _quarantine_ts = src._quarantine_ts
    WHEN NOT MATCHED THEN
INSERT (raw_payload, error_reason, _quarantine_ts)
VALUES (
        src.raw_payload,
        src.error_reason,
        src._quarantine_ts
    );