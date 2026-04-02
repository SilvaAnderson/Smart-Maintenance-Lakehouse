-- Data Quality para camada Silver (Expectations style)
USE CATALOG hive_metastore;
USE smart_maintenance;
CREATE TABLE IF NOT EXISTS silver_ai4i2020_quality_audit (
    audit_ts TIMESTAMP,
    total_rows BIGINT,
    null_key_rows BIGINT,
    invalid_air_temp_rows BIGINT,
    invalid_process_temp_rows BIGINT,
    invalid_rotation_rows BIGINT,
    invalid_torque_rows BIGINT,
    invalid_tool_wear_rows BIGINT,
    quality_status STRING
) USING DELTA;
INSERT INTO silver_ai4i2020_quality_audit
SELECT current_timestamp() AS audit_ts,
    COUNT(*) AS total_rows,
    SUM(
        CASE
            WHEN udi IS NULL
            OR product_id IS NULL
            OR machine_type IS NULL
            OR log_ts IS NULL THEN 1
            ELSE 0
        END
    ) AS null_key_rows,
    SUM(
        CASE
            WHEN air_temperature_k < 250
            OR air_temperature_k > 350 THEN 1
            ELSE 0
        END
    ) AS invalid_air_temp_rows,
    SUM(
        CASE
            WHEN process_temperature_k < 250
            OR process_temperature_k > 400 THEN 1
            ELSE 0
        END
    ) AS invalid_process_temp_rows,
    SUM(
        CASE
            WHEN rotational_speed_rpm <= 0 THEN 1
            ELSE 0
        END
    ) AS invalid_rotation_rows,
    SUM(
        CASE
            WHEN torque_nm < 0 THEN 1
            ELSE 0
        END
    ) AS invalid_torque_rows,
    SUM(
        CASE
            WHEN tool_wear_min < 0 THEN 1
            ELSE 0
        END
    ) AS invalid_tool_wear_rows,
    CASE
        WHEN SUM(
            CASE
                WHEN udi IS NULL
                OR product_id IS NULL
                OR machine_type IS NULL
                OR log_ts IS NULL THEN 1
                ELSE 0
            END
        ) = 0
        AND SUM(
            CASE
                WHEN air_temperature_k < 250
                OR air_temperature_k > 350 THEN 1
                ELSE 0
            END
        ) = 0
        AND SUM(
            CASE
                WHEN process_temperature_k < 250
                OR process_temperature_k > 400 THEN 1
                ELSE 0
            END
        ) = 0
        AND SUM(
            CASE
                WHEN rotational_speed_rpm <= 0 THEN 1
                ELSE 0
            END
        ) = 0
        AND SUM(
            CASE
                WHEN torque_nm < 0 THEN 1
                ELSE 0
            END
        ) = 0
        AND SUM(
            CASE
                WHEN tool_wear_min < 0 THEN 1
                ELSE 0
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS quality_status
FROM silver_ai4i2020_validated;
-- Otimização de leitura da Silver junto da transformação
OPTIMIZE silver_ai4i2020_validated ZORDER BY (product_id, log_ts);