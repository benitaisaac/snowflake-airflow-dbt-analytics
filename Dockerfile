FROM apache/airflow:2.10.1

# upgrade pip (helps resolver)
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# install airflow deps + yfinance first
RUN pip install --no-cache-dir \
    yfinance==0.2.66 \
    apache-airflow-providers-snowflake==5.7.0

# now install dbt and let pip pick the protobuf version
RUN pip install --no-cache-dir --pre dbt-snowflake
