# snowflake-airflow-dbt-analytics
End-to-end data analytics pipeline using Airflow, Snowflake, dbt, and Superset for stock price analytics.

# 🧠 Lab 2S – End-to-End Stock Analytics Pipeline
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

### 1️⃣ Airflow – ETL
**File:** `airflow/dags/two_stock_simple.py`  
- Extracts stock data (DIS, NFLX by default) from `yfinance`.  
- Transforms it (clean columns, types).  
- Loads into Snowflake table `RAW.TWO_STOCK_V2`.  
- Uses Airflow Variables:
  - `stock_symbols` → `DIS,NFLX`
  - `lookback_days` → `180`
- Uses Connection ` snowflake_conn `.

### 2️⃣ Airflow – ML Forecast
**File:** `airflow/dags/TrainPredict.py`  
- Creates Snowflake ML Forecast function (`analytics.predict_two_stock_price`).  
- Generates 7-day forecast and creates final table `analytics.two_stock`.  
- Triggered automatically after ETL DAG completion.

### 3️⃣ dbt – ELT Transformations (NEW for Lab 2)
**Folder:** `dbt/stock_analytics/`  
- Cleans and aggregates data from `RAW` and `ANALYTICS` schemas.  
- Example models:
  - `stg_stock_data.sql` → standardizes columns and dates  
  - `fct_stock_metrics.sql` → calculates moving averages, RSI, daily returns  
- Add tests in `schema.yml` and schedule dbt runs in Airflow.

### 4️⃣ BI Visualization (NEW for Lab 2)
Use Superset (or Preset/Tableau) connected to Snowflake:
- Build interactive dashboards for:
  - Stock trends and 7-day forecasts  
  - Moving averages and momentum  
  - RSI and price volatility  
- Include date filters and symbol selectors.

---

## ⚙️ Setup & Run
### 1. Clone Repo
```bash
git clone https://github.com/yourusername/data226-stock-analytics.git
cd data226-stock-analytics

