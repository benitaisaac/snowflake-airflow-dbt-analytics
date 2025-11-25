# End-to-End Stock Analytics Pipeline
**Course:** DATA 226  
**Authors:** Benita Isaac and Shruthi Thirukumaran
**Tools:** Airflow • Snowflake • dbt • Superset (or Preset/Tableau)

---

## 🎯 Objective
Build an **end-to-end data analytics system** that extracts stock-price data, loads it into Snowflake, trains a forecasting model, transforms data with dbt, and visualizes key metrics in a BI tool.  
This lab extends **Lab 1 (ETL + Forecast)** by adding **ELT (db t models)** and **Visualization** components.

---
## System Architecture 



---

## 🧩 Components

### 1️⃣ Airflow – ETL Pipeline
**File:** `airflow/dags/stock_price_etl_dag.py`  
- Extracts historical stock data from the **yfinance** API (e.g., DIS and NFLX).  
- Transforms the raw API output into a standardized tabular format.  
- Loads data into the Snowflake table `RAW.TWO_STOCK_V2`.  
- Ensures **idempotent loading** using a staging table and a MERGE-based upsert pattern.  
- Uses Airflow configuration elements:  
  - **Variables**:  
    - `stock_symbols` → `"DIS,NFLX"`  
    - `lookback_days` → `180`  
  - **Connection**:  
    - `snowflake_conn` → Snowflake authentication and warehouse settings.

---

### 2️⃣ Airflow – dbt ELT Pipeline
**File:** `airflow/dags/dbt_elt_dag.py`  
- Executes dbt inside the Airflow container using a dedicated virtual environment.  
- Runs the ELT workflow in sequence:  
  1. `dbt run` → builds **staging** and **fact** models in Snowflake.  
  2. `dbt test` → validates data quality (not-null, unique, relationship tests).  
  3. `dbt snapshot` → records historical versions of stock data using dbt snapshots.  
- Configured to run **automatically after** the ETL pipeline via `TriggerDagRunOperator`.  
- Uses:  
  - dbt venv at `/home/airflow/dbtvenv`  
  - Mounted dbt project at `/opt/airflow/dbt`

---

### 3️⃣ dbt – Modeling, Testing, and Snapshots
**Folder:** `dbt/stock_analytics/`  
- Implements SQL-based transformations inside Snowflake.  
- Core models:  
  - `stg_stock_data.sql` → cleans and standardizes raw stock data.  
  - `fct_stock_metrics.sql` → computes 7-day/30-day moving averages and daily percent change.  
- **Tests** defined in `schema.yml`:  
  - `not_null`, `unique`, `relationships`, and custom constraints.  
- **Snapshots** (e.g., `stock_prices_snapshot.sql`) track historical record changes over time.

---

### 4️⃣ BI Visualization – Preset (Superset)
- Connects directly to Snowflake’s **ANALYTICS** schema.  
- Interactive dashboard includes:  
  - Line chart for daily closing price and moving averages.  
  - 7-day moving average trend chart.  
  - Monthly aggregated percentage-change bar chart.  
- Symbol filtering (e.g., DIS vs. NFLX) demonstrates dashboard interactivity.  
- Validates that ELT outputs are accurate and suitable for analysis.


---

## ⚙️ Setup & Run
### 1. Clone Repo
```bash
git clone https://github.com/yourusername/data226-stock-analytics.git
cd data226-stock-analytics

