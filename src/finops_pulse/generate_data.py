import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

SERVICES = ["Compute", "Storage", "SQL", "Networking", "AI", "Monitoring"]
RESOURCE_GROUPS = ["rg-core", "rg-analytics", "rg-ml", "rg-web", "rg-data"]
SUBSCRIPTIONS = ["sub-prod", "sub-dev"]

SERVICE_BASE = {"Compute": 120, "Storage": 55, "SQL": 80, "Networking": 25, "AI": 35, "Monitoring": 18}
SERVICE_VOL  = {"Compute": 0.10, "Storage": 0.05, "SQL": 0.08, "Networking": 0.06, "AI": 0.14, "Monitoring": 0.06}

def _date_range(end_date: datetime, days: int):
    return [end_date - timedelta(days=i) for i in range(days)][::-1]

def generate(days: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    end = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    dates = _date_range(end, days)

    # --- Global latent factors to create "organic" correlated movement ---
    # AR(1) style demand factor: yesterday influences today
    demand = []
    x = 0.0
    for _ in dates:
        x = 0.85 * x + rng.normal(0, 0.25)  # smooth correlation over time
        demand.append(x)
    demand = np.array(demand)

    # Monthly cycle (end-of-month bump)
    month_cycle = np.array([1.0 + (0.12 if d.day >= 25 else 0.0) + rng.normal(0, 0.02) for d in dates])

    # Step-change events (simulates a deployment or growth starting a day)
    # Example: from a given date onwards, prod compute + sql increase in rg-core and rg-data
    step_start_idx = int(len(dates) * 0.55)  # mid-series
    step_multiplier = np.ones(len(dates))
    step_multiplier[step_start_idx:] = 1.12  # permanent 12% uplift after rollout

    rows = []
    for i, d in enumerate(dates):
        dow = d.weekday()
        weekday_factor = 1.10 if dow < 5 else 0.85  # clear but realistic weekday/weekend
        # overall day factor
        day_factor = weekday_factor * month_cycle[i] * (1.0 + 0.05 * demand[i])

        for sub in SUBSCRIPTIONS:
            sub_factor = 1.18 if sub == "sub-prod" else 0.60

            for rg in RESOURCE_GROUPS:
                rg_factor = {
                    "rg-core": 1.20,
                    "rg-analytics": 1.00,
                    "rg-ml": 0.95,
                    "rg-web": 0.85,
                    "rg-data": 1.05
                }[rg]

                # correlated “cluster load” per RG (adds organic movement)
                rg_load = 1.0 + rng.normal(0, 0.03) + (0.03 * demand[i])

                for svc in SERVICES:
                    base = SERVICE_BASE[svc]
                    vol = SERVICE_VOL[svc]

                    # service correlation: compute affects networking/monitoring naturally
                    service_corr = 1.0
                    if svc in ["Networking", "Monitoring"]:
                        service_corr += 0.06 * demand[i]
                    if svc == "AI":
                        service_corr += 0.08 * demand[i]

                    # slow trend growth (tiny)
                    trend = 1.0 + 0.0010 * i

                    # step-change affects specific slice (feels real)
                    rollout_factor = 1.0
                    if sub == "sub-prod" and rg in ["rg-core", "rg-data"] and svc in ["Compute", "SQL"]:
                        rollout_factor = step_multiplier[i]

                    # heteroscedastic noise: bigger base => bigger absolute variation
                    noise = rng.normal(0, base * vol)

                    cost = base * sub_factor * rg_factor * day_factor * rg_load * service_corr * trend * rollout_factor + noise
                    cost = max(0.0, cost)

                    rows.append([d.date().isoformat(), sub, rg, svc, round(cost, 2)])

    df = pd.DataFrame(rows, columns=["date", "subscription", "resource_group", "service", "cost"])

    # --- Incident-like anomalies (spike + recovery) ---
    anomaly_days = rng.choice(sorted(df["date"].unique()), size=3, replace=False)
    for ad in anomaly_days:
        # 1) incident spike day
        mask = (
            (df["date"] == ad) &
            (df["subscription"] == "sub-prod") &
            (df["resource_group"].isin(["rg-core", "rg-data"])) &
            (df["service"].isin(["Compute", "SQL", "Networking"]))
        )
        df.loc[mask, "cost"] *= rng.uniform(1.8, 2.6)

        # 2) partial recovery next day (feels more organic than instant drop)
        ad_dt = datetime.fromisoformat(ad)
        next_day = (ad_dt + timedelta(days=1)).date().isoformat()
        mask2 = (
            (df["date"] == next_day) &
            (df["subscription"] == "sub-prod") &
            (df["resource_group"].isin(["rg-core", "rg-data"])) &
            (df["service"].isin(["Compute", "SQL", "Networking"]))
        )
        df.loc[mask2, "cost"] *= rng.uniform(1.15, 1.35)

    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--out", type=str, default="data/raw/daily_cost.csv")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    df = generate(days=args.days, seed=args.seed)
    df.to_csv(args.out, index=False)
    print(f"Generated {len(df):,} rows -> {args.out}")
    print(df.head(5).to_string(index=False))

if __name__ == "__main__":
    main()
