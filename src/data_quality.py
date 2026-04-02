from typing import Dict
import pandas as pd


def evaluate_silver_quality(df: pd.DataFrame) -> Dict[str, int | str]:
    total_rows = int(len(df))

    null_key_rows = int(
        df[["udi", "product_id", "machine_type", "log_ts"]].isnull().any(axis=1).sum()
    )
    invalid_air_temp_rows = int(((df["air_temperature_k"] < 250) | (df["air_temperature_k"] > 350)).sum())
    invalid_process_temp_rows = int(((df["process_temperature_k"] < 250) | (df["process_temperature_k"] > 400)).sum())
    invalid_rotation_rows = int((df["rotational_speed_rpm"] <= 0).sum())
    invalid_torque_rows = int((df["torque_nm"] < 0).sum())
    invalid_tool_wear_rows = int((df["tool_wear_min"] < 0).sum())

    quality_status = "PASS" if all([
        null_key_rows == 0,
        invalid_air_temp_rows == 0,
        invalid_process_temp_rows == 0,
        invalid_rotation_rows == 0,
        invalid_torque_rows == 0,
        invalid_tool_wear_rows == 0,
    ]) else "FAIL"

    return {
        "total_rows": total_rows,
        "null_key_rows": null_key_rows,
        "invalid_air_temp_rows": invalid_air_temp_rows,
        "invalid_process_temp_rows": invalid_process_temp_rows,
        "invalid_rotation_rows": invalid_rotation_rows,
        "invalid_torque_rows": invalid_torque_rows,
        "invalid_tool_wear_rows": invalid_tool_wear_rows,
        "quality_status": quality_status,
    }
