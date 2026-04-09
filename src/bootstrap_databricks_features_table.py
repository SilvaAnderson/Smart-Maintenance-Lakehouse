from databricks import sql
import pandas as pd
from pathlib import Path
import tomllib

ROOT = Path(__file__).resolve().parents[1]
SECRETS_FILE = ROOT / ".streamlit" / "secrets.toml"
DATA_FILE = ROOT / "ai4i2020.xls"
TARGET_TABLE = "workspace.smart_maintenance.gold_ai4i2020_features"


def main() -> None:
    secrets = tomllib.loads(SECRETS_FILE.read_text(encoding="utf-8"))
    token = secrets["DATABRICKS_ACCESS_TOKEN"].strip()
    if token.startswith("Sdapi"):
        token = token[1:]

    df = None
    last_errors = []
    for excel_engine in ("openpyxl", "xlrd"):
        try:
            df = pd.read_excel(DATA_FILE, engine=excel_engine)
            break
        except Exception as exc:
            last_errors.append(f"excel:{excel_engine}:{exc}")

    if df is None:
        try:
            df = pd.read_csv(DATA_FILE, sep=None, engine="python")
        except Exception as exc:
            last_errors.append(f"csv:{exc}")
            raise RuntimeError("Falha ao ler ai4i2020.xls") from exc
    df = df.rename(
        columns={
            "UDI": "udi",
            "\ufeffUDI": "udi",
            "Product ID": "product_id",
            "Type": "machine_type",
            "Air temperature [K]": "air_temperature_k",
            "Process temperature [K]": "process_temperature_k",
            "Rotational speed [rpm]": "rotational_speed_rpm",
            "Torque [Nm]": "torque_nm",
            "Tool wear [min]": "tool_wear_min",
            "Machine failure": "machine_failure",
            "TWF": "twf_label",
            "HDF": "hdf_label",
            "PWF": "pwf_label",
            "OSF": "osf_label",
            "RNF": "rnf_label",
        }
    )

    df["event_date"] = pd.Timestamp.utcnow().date()
    df["event_hour"] = pd.Timestamp.utcnow().hour

    cols = [
        "udi",
        "product_id",
        "machine_type",
        "event_date",
        "event_hour",
        "air_temperature_k",
        "process_temperature_k",
        "rotational_speed_rpm",
        "torque_nm",
        "tool_wear_min",
        "machine_failure",
        "twf_label",
        "hdf_label",
        "pwf_label",
        "osf_label",
        "rnf_label",
    ]

    conn = sql.connect(
        server_hostname=secrets["DATABRICKS_SERVER_HOSTNAME"].replace("https://", "").replace("http://", "").strip(),
        http_path=secrets["DATABRICKS_HTTP_PATH"].strip(),
        access_token=token,
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS workspace.smart_maintenance")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS workspace.smart_maintenance.gold_ai4i2020_features (
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
                    rnf_label INT
                ) USING DELTA
                """
            )
            cur.execute(f"TRUNCATE TABLE {TARGET_TABLE}")

            insert_sql = f"""
                INSERT INTO {TARGET_TABLE} (
                    udi, product_id, machine_type, event_date, event_hour,
                    air_temperature_k, process_temperature_k, rotational_speed_rpm,
                    torque_nm, tool_wear_min, machine_failure,
                    twf_label, hdf_label, pwf_label, osf_label, rnf_label
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            batch_size = 1000
            rows = [tuple(row[col] for col in cols) for _, row in df.iterrows()]
            for start in range(0, len(rows), batch_size):
                batch = rows[start : start + batch_size]
                cur.executemany(insert_sql, batch)

    print(f"Tabela carregada com sucesso: {TARGET_TABLE} ({len(df)} linhas)")


if __name__ == "__main__":
    main()
