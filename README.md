# FinOps Pulse — Cloud Spend Anomaly Detection & Auto-Explanation

FinOps Pulse is a small FinOps monitoring tool that:
1) detects unusual cloud spend spikes, and  
2) automatically explains *what caused the spike* (top services + resource groups).

It produces clean CSV outputs and a Streamlit dashboard so anyone can investigate quickly.

---

## 1) Problem Statement (in simple words)

Cloud spending can jump suddenly because of:
- runaway compute jobs
- unexpected scaling
- new deployments
- inefficient database queries
- increased traffic

When the bill spikes, teams want answers fast:

- **Is today’s spend unusually high?**
- **How big is the spike?**
- **Which services caused it?** (Compute, SQL, Storage…)
- **Which resource groups caused it?** (rg-core, rg-data…)
- **Where should we start investigating?**

FinOps Pulse automates this investigation.

---

## 2) What FinOps Pulse Builds (Outputs)

After you run the pipeline, you get:

### A) Aggregated tables (for dashboards)
- `data/processed/daily_total.csv` → total spend per day  
- `data/processed/daily_by_service.csv` → spend per day per service  
- `data/processed/daily_by_rg.csv` → spend per day per resource group  

### B) Anomaly detection output
- `reports/anomalies.csv` → all detected anomalies (total/service/resource group)

### C) Auto-explanations
- `reports/anomaly_explanations.csv` → for each anomaly date, top drivers (services + RGs)

### D) A simple dashboard
- Streamlit UI in `app.py` showing:
  - total spend chart
  - anomaly table
  - explanation dropdown + driver breakdown

---

## 3) How It Works (Step-by-Step)

### Step 1 — Input Data
We start from daily cost rows with:
- `date`
- `subscription`
- `resource_group`
- `service`
- `cost`

For this demo, we generate **synthetic but realistic** cost data (because real billing data is private).

### Step 2 — Aggregation (Make data useful)
We summarize raw rows into daily tables:
- daily total spend
- spend by service
- spend by resource group

### Step 3 — Anomaly Detection (Find spikes)
We compare today’s spend against “normal spend” from recent history.

- **Baseline** = median of last 14 days  
- **MAD** = how much spend normally varies (Median Absolute Deviation)  
- **Threshold** = baseline + multiplier × MAD  
- If `today > threshold` → anomaly

This is robust because the **median** is not distorted by spikes.

### Step 4 — Auto-Explanation (Find the cause)
For each anomaly day, we compute:
- which **services** increased the most vs normal (Compute, SQL, etc.)
- which **resource groups** increased the most vs normal (rg-core, rg-data, etc.)

This is what makes the project business-useful: it answers **“why did it spike?”**.

---

## 4) Folder Structure



## 5) Quick Start (Run in 3 Commands)

## 5.1 Setup environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
````

## 5.2 Run pipeline (generate → aggregate → detect → explain)

```bash
PYTHONPATH=src python -m finops_pulse.run_all
```

## 5.3 Run dashboard

```bash
PYTHONPATH=src streamlit run app.py
```

Open:

* [http://localhost:8501](http://localhost:8501)

## 6) How to Read the Key Terms (No jargon)
In reports/anomalies.csv: 

    1. Actual Spend (value): spend on that day

    2. Expected (baseline): “normal” spend (median of recent days)

    3. Delta: extra spend = actual − expected

    4. Threshold: cutoff above which it’s considered unusual

    5. Scope (level): Total / Service / Resource Group

    6. Key: which service or RG spiked

In reports/anomaly_explanations.csv

    1. top_service_1 / service_delta_1: biggest service contributor and its extra spend vs normal

    2. top_rg_1 / rg_delta_1: biggest RG contributor and its extra spend vs normal

Note: Service and resource group views are different slices of the same spend.
You should not add service deltas + RG deltas together.

## 7) Tuning Sensitivity (More or fewer anomalies)

In src/finops_pulse/run_all.py:

   mad_multiplier controls sensitivity

          1. higher → fewer anomalies

          2. lower → more anomalies

Example:

cfg = AnomalyConfig(window_days=14, mad_multiplier=6.0, min_history=21)

