from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import pandas as pd
import os

# Pode ser sobrescrito por variáveis de ambiente:
# AI4I_SOURCE_FILE, AI4I_TARGET_CATALOG, AI4I_TARGET_SCHEMA
SOURCE_FILE = os.getenv("AI4I_SOURCE_FILE", "/dbfs/FileStore/ai4i2020.xls")
TARGET_CATALOG = os.getenv("AI4I_TARGET_CATALOG", "hive_metastore")
TARGET_SCHEMA = os.getenv("AI4I_TARGET_SCHEMA", "smart_maintenance")
BRONZE_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.bronze_ai4i2020_raw"

spark = SparkSession.builder.getOrCreate()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")

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

print(f"Bronze carregada com sucesso em: {BRONZE_TABLE}")
