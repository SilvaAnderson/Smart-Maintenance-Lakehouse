from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T
import os
import re
from pathlib import Path

spark = SparkSession.builder.getOrCreate()

SOURCE_FILE = os.getenv("AI4I_SOURCE_FILE", "/dbfs/FileStore/ai4i2020.xls")
TARGET_CATALOG = os.getenv("AI4I_TARGET_CATALOG", "hive_metastore")
TARGET_SCHEMA = os.getenv("AI4I_TARGET_SCHEMA", "smart_maintenance")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = Path(os.getenv("AI4I_SQL_DIR", str(PROJECT_ROOT / "sql")))

BRONZE_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.bronze_ai4i2020_raw"


def run_sql_script(file_path: str) -> None:
    with open(file_path, "r", encoding="utf-8") as f:
        sql_text = f.read()

    sql_text = re.sub(r"USE\s+CATALOG\s+hive_metastore\s*;", f"USE CATALOG {TARGET_CATALOG};", sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r"CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+smart_maintenance\s*;", f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};", sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r"USE\s+smart_maintenance\s*;", f"USE {TARGET_SCHEMA};", sql_text, flags=re.IGNORECASE)

    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]

    for stmt in statements:
        spark.sql(stmt)


def ingest_bronze() -> None:
    spark.sql(f"USE CATALOG {TARGET_CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")

    pdf = pd.read_excel(SOURCE_FILE)

    rename_map = {
        "UDI": "udi",
        "Product ID": "product_id",
        "Type": "machine_type",
        "Air temperature [K]": "air_temperature_k",
        "Process temperature [K]": "process_temperature_k",
        "Rotational speed [rpm]": "rotational_speed_rpm",
        "Torque [Nm]": "torque_nm",
        "Tool wear [min]": "tool_wear_min",
        "Machine failure": "machine_failure",
        "TWF": "twf",
        "HDF": "hdf",
        "PWF": "pwf",
        "OSF": "osf",
        "RNF": "rnf"
    }

    pdf = pdf.rename(columns=rename_map)

    schema = T.StructType([
        T.StructField("udi", T.IntegerType(), True),
        T.StructField("product_id", T.StringType(), True),
        T.StructField("machine_type", T.StringType(), True),
        T.StructField("air_temperature_k", T.DoubleType(), True),
        T.StructField("process_temperature_k", T.DoubleType(), True),
        T.StructField("rotational_speed_rpm", T.IntegerType(), True),
        T.StructField("torque_nm", T.DoubleType(), True),
        T.StructField("tool_wear_min", T.IntegerType(), True),
        T.StructField("machine_failure", T.IntegerType(), True),
        T.StructField("twf", T.IntegerType(), True),
        T.StructField("hdf", T.IntegerType(), True),
        T.StructField("pwf", T.IntegerType(), True),
        T.StructField("osf", T.IntegerType(), True),
        T.StructField("rnf", T.IntegerType(), True)
    ])

    base_df = spark.createDataFrame(pdf, schema=schema)

    bronze_df = (
        base_df
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_file", F.lit(SOURCE_FILE))
        .withColumn("log_ts", F.current_timestamp())
        .withColumn("ingestion_date", F.to_date("ingestion_ts"))
    )

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .saveAsTable(BRONZE_TABLE)
    )

    spark.sql(f"ALTER TABLE {BRONZE_TABLE} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")


if __name__ == "__main__":
    ingest_bronze()

    run_sql_script(str(SQL_DIR / "02_silver_validated.sql"))
    run_sql_script(str(SQL_DIR / "02a_silver_data_quality.sql"))
    run_sql_script(str(SQL_DIR / "03_gold_analytics.sql"))
    run_sql_script(str(SQL_DIR / "04_cdf_auditoria.sql"))
    run_sql_script(str(SQL_DIR / "05_otimizacao_time_travel.sql"))

    print("Pipeline Medallion executado com sucesso (Bronze -> Silver -> Gold -> CDF -> Otimização/Time Travel).")
