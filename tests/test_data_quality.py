import pandas as pd

from src.data_quality import evaluate_silver_quality


def test_quality_pass_for_valid_rows() -> None:
    df = pd.DataFrame([
        {
            "udi": 1,
            "product_id": "M1001",
            "machine_type": "M",
            "log_ts": "2026-04-02T00:00:00",
            "air_temperature_k": 300.0,
            "process_temperature_k": 310.0,
            "rotational_speed_rpm": 1500,
            "torque_nm": 40.0,
            "tool_wear_min": 50,
        }
    ])

    result = evaluate_silver_quality(df)

    assert result["quality_status"] == "PASS"
    assert result["null_key_rows"] == 0
    assert result["invalid_torque_rows"] == 0


def test_quality_fail_for_invalid_sensor_values() -> None:
    df = pd.DataFrame([
        {
            "udi": None,
            "product_id": "M1002",
            "machine_type": "L",
            "log_ts": "2026-04-02T00:00:00",
            "air_temperature_k": 500.0,
            "process_temperature_k": 100.0,
            "rotational_speed_rpm": 0,
            "torque_nm": -1.0,
            "tool_wear_min": -10,
        }
    ])

    result = evaluate_silver_quality(df)

    assert result["quality_status"] == "FAIL"
    assert result["null_key_rows"] == 1
    assert result["invalid_air_temp_rows"] == 1
    assert result["invalid_process_temp_rows"] == 1
    assert result["invalid_rotation_rows"] == 1
    assert result["invalid_torque_rows"] == 1
    assert result["invalid_tool_wear_rows"] == 1
