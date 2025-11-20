FROM apache/airflow:2.10.1

# Work as airflow user (recommended by Airflow images)
USER airflow

# Upgrade pip and install Python deps you need in the base env
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir \
        yfinance==0.2.66 \
        apache-airflow-providers-snowflake==5.7.0

# Create dbt venv and install dbt there (pin version)
RUN python -m venv /home/airflow/dbtvenv && \
    . /home/airflow/dbtvenv/bin/activate && \
    pip install --upgrade pip && \
    pip install "dbt-snowflake==1.7.*"

# Back to airflow user already; no change needed
