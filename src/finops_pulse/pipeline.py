import pandas as pd
import numpy as np
from dataclasses import dataclass

@dataclass
class AnomalyConfig:
    window_days: int = 14
    mad_multiplier: float = 8.0  # higher => fewer false positives
    min_history: int = 21

def _median_abs_deviation(x: np.ndarray) -> float:
    med = np.median(x)
    return np.median(np.abs(x - med))

def _detect_mad(series: pd.Series, cfg: AnomalyConfig) -> pd.DataFrame:
    """
    Rolling median + MAD based anomaly detection.
    Returns rows where today's value exceeds baseline by threshold.
    """
    s = series.astype(float)
    out_rows = []

    for i in range(len(s)):
        if i < cfg.min_history:
            continue

        hist = s.iloc[max(0, i - cfg.window_days):i].values
        baseline = float(np.median(hist))
        mad = float(_median_abs_deviation(hist))
        # avoid division by zero / overly strict thresholds
        mad = mad if mad > 1e-6 else 1.0

        value = float(s.iloc[i])
        threshold = baseline + cfg.mad_multiplier * mad
        is_anomaly = value > threshold

        if is_anomaly:
            out_rows.append({
                "date": s.index[i],
                "value": round(value, 2),
                "baseline": round(baseline, 2),
                "mad": round(mad, 2),
                "threshold": round(threshold, 2),
                "delta": round(value - baseline, 2),
            })

    return pd.DataFrame(out_rows)

def build_aggregates(df: pd.DataFrame):
    df["date"] = pd.to_datetime(df["date"])

    daily_total = df.groupby("date", as_index=False)["cost"].sum().rename(columns={"cost": "total_cost"})
    daily_by_service = df.groupby(["date", "service"], as_index=False)["cost"].sum()
    daily_by_rg = df.groupby(["date", "resource_group"], as_index=False)["cost"].sum()

    return daily_total, daily_by_service, daily_by_rg

def detect_anomalies(daily_total: pd.DataFrame,
                     daily_by_service: pd.DataFrame,
                     daily_by_rg: pd.DataFrame,
                     cfg: AnomalyConfig) -> pd.DataFrame:
    # Total anomalies
    s_total = daily_total.set_index("date")["total_cost"]
    a_total = _detect_mad(s_total, cfg)
    if not a_total.empty:
        a_total["level"] = "total"
        a_total["key"] = "all"

    # Service anomalies
    service_rows = []
    for svc, grp in daily_by_service.groupby("service"):
        s = grp.sort_values("date").set_index("date")["cost"]
        a = _detect_mad(s, cfg)
        if not a.empty:
            a["level"] = "service"
            a["key"] = svc
            service_rows.append(a)
    a_service = pd.concat(service_rows, ignore_index=True) if service_rows else pd.DataFrame()

    # Resource group anomalies
    rg_rows = []
    for rg, grp in daily_by_rg.groupby("resource_group"):
        s = grp.sort_values("date").set_index("date")["cost"]
        a = _detect_mad(s, cfg)
        if not a.empty:
            a["level"] = "resource_group"
            a["key"] = rg
            rg_rows.append(a)
    a_rg = pd.concat(rg_rows, ignore_index=True) if rg_rows else pd.DataFrame()

    anomalies = pd.concat([a_total, a_service, a_rg], ignore_index=True) if not a_total.empty or not a_service.empty or not a_rg.empty else pd.DataFrame()
    if anomalies.empty:
        return anomalies

    anomalies["date"] = pd.to_datetime(anomalies["date"])
    anomalies = anomalies.sort_values(["date", "level", "delta"], ascending=[True, True, False])
    return anomalies

def explain_total_anomalies(daily_by_service: pd.DataFrame,
                            daily_by_rg: pd.DataFrame,
                            total_anomalies: pd.DataFrame,
                            top_k: int = 3) -> pd.DataFrame:
    """
    For each total anomaly day, compute top drivers by comparing the anomaly day cost
    vs baseline (median of previous 14 days) for each service and resource group.
    """
    if total_anomalies.empty:
        return pd.DataFrame()

    daily_by_service = daily_by_service.copy()
    daily_by_rg = daily_by_rg.copy()
    daily_by_service["date"] = pd.to_datetime(daily_by_service["date"])
    daily_by_rg["date"] = pd.to_datetime(daily_by_rg["date"])

    results = []

    for _, row in total_anomalies.iterrows():
        day = pd.to_datetime(row["date"])
        start = day - pd.Timedelta(days=14)

        svc_hist = daily_by_service[(daily_by_service["date"] >= start) & (daily_by_service["date"] < day)]
        rg_hist = daily_by_rg[(daily_by_rg["date"] >= start) & (daily_by_rg["date"] < day)]

        svc_today = daily_by_service[daily_by_service["date"] == day]
        rg_today = daily_by_rg[daily_by_rg["date"] == day]

        # Baselines
        svc_base = svc_hist.groupby("service")["cost"].median()
        rg_base = rg_hist.groupby("resource_group")["cost"].median()

        svc_delta = []
        for _, r in svc_today.iterrows():
            base = float(svc_base.get(r["service"], 0.0))
            svc_delta.append((r["service"], float(r["cost"]) - base))

        rg_delta = []
        for _, r in rg_today.iterrows():
            base = float(rg_base.get(r["resource_group"], 0.0))
            rg_delta.append((r["resource_group"], float(r["cost"]) - base))

        svc_delta.sort(key=lambda x: x[1], reverse=True)
        rg_delta.sort(key=lambda x: x[1], reverse=True)

        rec = {"date": day.date().isoformat()}
        for i in range(top_k):
            if i < len(svc_delta):
                rec[f"top_service_{i+1}"] = svc_delta[i][0]
                rec[f"service_delta_{i+1}"] = round(svc_delta[i][1], 2)
            else:
                rec[f"top_service_{i+1}"] = ""
                rec[f"service_delta_{i+1}"] = 0.0

        for i in range(top_k):
            if i < len(rg_delta):
                rec[f"top_rg_{i+1}"] = rg_delta[i][0]
                rec[f"rg_delta_{i+1}"] = round(rg_delta[i][1], 2)
            else:
                rec[f"top_rg_{i+1}"] = ""
                rec[f"rg_delta_{i+1}"] = 0.0

        results.append(rec)

    return pd.DataFrame(results)
