from pathlib import Path
import json
import os

import joblib
import pandas as pd
from sklearn.metrics import f1_score
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputClassifier
from xgboost import XGBClassifier

DATA_FILE = os.getenv("AI4I_TRAINING_FILE", str(Path(__file__).resolve().parents[1] / "ai4i2020.xls"))
ARTIFACT_DIR = Path(os.getenv("AI4I_MODEL_DIR", str(Path(__file__).resolve().parents[1] / "artifacts")))

FEATURE_COLUMNS = [
    "Air temperature [K]",
    "Process temperature [K]",
    "Rotational speed [rpm]",
    "Torque [Nm]",
    "Tool wear [min]",
]
TARGET_COLUMNS = ["TWF", "HDF", "PWF", "OSF", "RNF"]


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(DATA_FILE)
    x = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMNS]

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, random_state=42, stratify=df["Machine failure"]
    )

    base_model = XGBClassifier(
        n_estimators=150,
        max_depth=5,
        learning_rate=0.08,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=42,
    )
    model = MultiOutputClassifier(base_model)
    model.fit(x_train, y_train)

    predictions = model.predict(x_test)
    metrics = {}
    for idx, target in enumerate(TARGET_COLUMNS):
        metrics[target] = {
            "f1": float(f1_score(y_test.iloc[:, idx], predictions[:, idx], zero_division=0))
        }

    joblib.dump(
        {
            "model": model,
            "feature_columns": FEATURE_COLUMNS,
            "target_columns": TARGET_COLUMNS,
        },
        ARTIFACT_DIR / "xgboost_multioutput.joblib",
    )

    with open(ARTIFACT_DIR / "xgboost_metrics.json", "w", encoding="utf-8") as fp:
        json.dump(metrics, fp, indent=2)

    print("Modelo XGBoost treinado com sucesso.")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
