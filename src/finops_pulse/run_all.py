import os
import pandas as pd
from finops_pulse.generate_data import generate
from finops_pulse.pipeline import build_aggregates, detect_anomalies, explain_total_anomalies, AnomalyConfig

RAW_PATH = "data/raw/daily_cost.csv"

def ensure_data():
    os.makedirs("data/raw", exist_ok=True)
    if not os.path.exists(RAW_PATH):
        df = generate(days=120, seed=42)
        df.to_csv(RAW_PATH, index=False)
        print(f"Created synthetic dataset: {RAW_PATH}")
    else:
        print(f"Found existing dataset: {RAW_PATH}")

def main():
    ensure_data()

    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("reports", exist_ok=True)

    df = pd.read_csv(RAW_PATH)

    daily_total, daily_by_service, daily_by_rg = build_aggregates(df)

    daily_total.to_csv("data/processed/daily_total.csv", index=False)
    daily_by_service.to_csv("data/processed/daily_by_service.csv", index=False)
    daily_by_rg.to_csv("data/processed/daily_by_rg.csv", index=False)

    cfg = AnomalyConfig(window_days=14, mad_multiplier=8.0, min_history=21)
    anomalies = detect_anomalies(daily_total, daily_by_service, daily_by_rg, cfg)

    if anomalies.empty:
        print("⚠️ No anomalies detected. Try lowering mad_multiplier in AnomalyConfig.")
        return

    anomalies.to_csv("reports/anomalies.csv", index=False)

    total_anoms = anomalies[anomalies["level"] == "total"].copy()
    explanations = explain_total_anomalies(daily_by_service, daily_by_rg, total_anoms, top_k=3)
    if not explanations.empty:
        explanations.to_csv("reports/anomaly_explanations.csv", index=False)

    print(f"Wrote aggregates -> data/processed/")
    print(f"Wrote anomalies -> reports/anomalies.csv ({len(anomalies)} rows)")
    if not explanations.empty:
        print(f"Wrote explanations -> reports/anomaly_explanations.csv ({len(explanations)} days)")

    # Print a quick human-readable summary
    if not explanations.empty:
        print("\n--- Sample Explanation ---")
        print(explanations.head(2).to_string(index=False))

if __name__ == "__main__":
    main()
