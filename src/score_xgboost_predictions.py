from pathlib import Path
import os

import joblib
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

TARGET_CATALOG = os.getenv("AI4I_TARGET_CATALOG", "hive_metastore")
TARGET_SCHEMA = os.getenv("AI4I_TARGET_SCHEMA", "smart_maintenance")
MODEL_DIR = Path(os.getenv("AI4I_MODEL_DIR", str(Path(__file__).resolve().parents[1] / "artifacts")))
MODEL_PATH = MODEL_DIR / "xgboost_multioutput.joblib"
FEATURE_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.gold_ai4i2020_features"
PRED_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.gold_ai4i2020_failure_predictions"


def main() -> None:
    artifact = joblib.load(MODEL_PATH)
    model = artifact["model"]
    feature_columns = artifact["feature_columns"]

    pdf = spark.table(FEATURE_TABLE).select("udi", "product_id", *feature_columns).toPandas()
    preds = model.predict(pdf[feature_columns])
    proba = []
    for row_idx in range(len(pdf)):
        max_score = 0.0
        for estimator_idx, estimator in enumerate(model.estimators_):
            score = float(estimator.predict_proba(pdf[feature_columns].iloc[[row_idx]])[0][1])
            max_score = max(max_score, score)
        proba.append(max_score)

    pdf["prediction_ts"] = pd_timestamp = None
    pred_df = spark.createDataFrame(
        pdf.assign(
            prediction_ts=None,
            pred_twf=preds[:, 0].tolist(),
            pred_hdf=preds[:, 1].tolist(),
            pred_pwf=preds[:, 2].tolist(),
            pred_osf=preds[:, 3].tolist(),
            pred_rnf=preds[:, 4].tolist(),
            model_name="xgboost_multioutput",
            model_version="v1",
            prediction_score=proba,
            _pred_loaded_ts=None,
        )[[
            "udi",
            "product_id",
            "prediction_ts",
            "pred_twf",
            "pred_hdf",
            "pred_pwf",
            "pred_osf",
            "pred_rnf",
            "model_name",
            "model_version",
            "prediction_score",
            "_pred_loaded_ts",
        ]]
    )

    pred_df = (
        pred_df
        .withColumn("prediction_ts", F.current_timestamp())
        .withColumn("_pred_loaded_ts", F.current_timestamp())
    )
    pred_df.createOrReplaceTempView("vw_model_predictions")

    spark.sql(
        f"""
        MERGE INTO {PRED_TABLE} AS tgt
        USING vw_model_predictions AS src
        ON tgt.udi = src.udi
        AND tgt.model_name = src.model_name
        AND tgt.model_version = src.model_version
        WHEN MATCHED THEN UPDATE SET
          product_id = src.product_id,
          prediction_ts = src.prediction_ts,
          pred_twf = src.pred_twf,
          pred_hdf = src.pred_hdf,
          pred_pwf = src.pred_pwf,
          pred_osf = src.pred_osf,
          pred_rnf = src.pred_rnf,
          prediction_score = src.prediction_score,
          _pred_loaded_ts = src._pred_loaded_ts
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    print(f"Predições gravadas em {PRED_TABLE}")


if __name__ == "__main__":
    main()
