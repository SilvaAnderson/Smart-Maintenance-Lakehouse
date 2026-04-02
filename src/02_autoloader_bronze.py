from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.getOrCreate()

TARGET_CATALOG = os.getenv("AI4I_TARGET_CATALOG", "hive_metastore")
TARGET_SCHEMA = os.getenv("AI4I_TARGET_SCHEMA", "smart_maintenance")
LANDING_PATH = os.getenv("AI4I_LANDING_PATH", "/Volumes/raw/ai4i2020_csv")
SCHEMA_PATH = os.getenv("AI4I_SCHEMA_PATH", "/Volumes/checkpoints/ai4i2020/schema")
CHECKPOINT_PATH = os.getenv("AI4I_CHECKPOINT_PATH", "/Volumes/checkpoints/ai4i2020/bronze")
BRONZE_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.bronze_ai4i2020_raw"


def main() -> None:
    spark.sql(f"USE CATALOG {TARGET_CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")

    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("header", "true")
        .load(LANDING_PATH)
        .withColumnRenamed("UDI", "udi")
        .withColumnRenamed("Product ID", "product_id")
        .withColumnRenamed("Type", "machine_type")
        .withColumnRenamed("Air temperature [K]", "air_temperature_k")
        .withColumnRenamed("Process temperature [K]", "process_temperature_k")
        .withColumnRenamed("Rotational speed [rpm]", "rotational_speed_rpm")
        .withColumnRenamed("Torque [Nm]", "torque_nm")
        .withColumnRenamed("Tool wear [min]", "tool_wear_min")
        .withColumnRenamed("Machine failure", "machine_failure")
        .withColumnRenamed("TWF", "twf")
        .withColumnRenamed("HDF", "hdf")
        .withColumnRenamed("PWF", "pwf")
        .withColumnRenamed("OSF", "osf")
        .withColumnRenamed("RNF", "rnf")
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("log_ts", F.current_timestamp())
        .withColumn("ingestion_date", F.to_date("ingestion_ts"))
    )

    (
        stream_df.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .partitionBy("ingestion_date")
        .toTable(BRONZE_TABLE)
    )

    spark.sql(f"ALTER TABLE {BRONZE_TABLE} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    print(f"Auto Loader executado com sucesso em {BRONZE_TABLE}")


if __name__ == "__main__":
    main()
