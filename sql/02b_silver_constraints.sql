-- Enforcement de qualidade na Silver com CHECK CONSTRAINT
USE CATALOG hive_metastore;
USE smart_maintenance;
ALTER TABLE silver_ai4i2020_validated
ADD CONSTRAINT valid_air_temperature CHECK (
        air_temperature_k BETWEEN 250 AND 350
    );
ALTER TABLE silver_ai4i2020_validated
ADD CONSTRAINT valid_process_temperature CHECK (
        process_temperature_k BETWEEN 250 AND 400
    );
ALTER TABLE silver_ai4i2020_validated
ADD CONSTRAINT valid_rotational_speed CHECK (rotational_speed_rpm > 0);
ALTER TABLE silver_ai4i2020_validated
ADD CONSTRAINT valid_torque CHECK (torque_nm >= 0);
ALTER TABLE silver_ai4i2020_validated
ADD CONSTRAINT valid_tool_wear CHECK (tool_wear_min >= 0);