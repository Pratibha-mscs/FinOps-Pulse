import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

SERVICES = ["Compute", "Storage", "SQL", "Networking", "AI", "Monitoring"]
RESOURCE_GROUPS = ["rg-core", "rg-analytics", "rg-ml", "rg-web", "rg-data"]
SUBSCRIPTIONS = ["sub-prod", "sub-dev"]

def _date_range(end_date: datetime, days: int):
    return [end_date - timedelta(days=i) for i in range(days)][::-1]

def generate(days: int, seed: int = 42) -> pd.DataFrame:
    """
    Create synthetic daily cost data with realistic patterns + injected anomalies.
    Columns: date, subscription, resource_group, service, cost
    """
    rng = np.random.default_rng(seed)
    end = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    dates = _date_range(end, days)

    rows = []
    for d in dates:
        dow = d.weekday()  # 0=Mon
        weekend_factor = 0.85 if dow >= 5 else 1.0

        for sub in SUBSCRIPTIONS:
            sub_factor = 1.15 if sub == "sub-prod" else 0.65

            for rg in RESOURCE_GROUPS:
                rg_factor = {
                    "rg-core": 1.2,
                    "rg-analytics": 1.0,
                    "rg-ml": 0.9,
                    "rg-web": 0.8,
                    "rg-data": 1.05
                }[rg]

                for svc in SERVICES:
                    base = {
                        "Compute": 120,
                        "Storage": 55,
                        "SQL": 80,
                        "Networking": 25,
                        "AI": 35,
                        "Monitoring": 18
                    }[svc]

                    # gentle seasonality/trend
                    day_index = (d - dates[0]).days
                    trend = 1.0 + 0.0015 * day_index
                    noise = rng.normal(0, 8)

                    cost = max(
                        0.0,
                        (base * sub_factor * rg_factor * weekend_factor * trend) + noise
                    )
                    rows.append([d.date().isoformat(), sub, rg, svc, round(cost, 2)])

    df = pd.DataFrame(rows, columns=["date", "subscription", "resource_group", "service", "cost"])

    # Inject a few anomaly days (spikes) on total spend via specific services/RGs
    anomaly_days = rng.choice(sorted(df["date"].unique()), size=4, replace=False)
    for ad in anomaly_days:
        # spike: compute + sql in prod/rg-core and prod/rg-data
        mask = (
            (df["date"] == ad) &
            (df["subscription"] == "sub-prod") &
            (df["resource_group"].isin(["rg-core", "rg-data"])) &
            (df["service"].isin(["Compute", "SQL"]))
        )
        df.loc[mask, "cost"] = df.loc[mask, "cost"] * rng.uniform(2.2, 3.2)

    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=120)
    parser.add_argument("--out", type=str, default="data/raw/daily_cost.csv")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    df = generate(days=args.days, seed=args.seed)
    df.to_csv(args.out, index=False)
    print(f"Generated {len(df):,} rows -> {args.out}")
    print("Sample:")
    print(df.head(5).to_string(index=False))

if __name__ == "__main__":
    main()
