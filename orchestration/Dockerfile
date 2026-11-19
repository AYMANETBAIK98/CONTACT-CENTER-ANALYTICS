# Airflow (step 8) with everything the daily batch needs: Java + PySpark for the lakehouse jobs,
# the project libraries, and dbt in its own virtualenv (its dependencies clash with Airflow's).
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless git \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
# Same Airflow version pinned, project libraries resolved against Airflow's official constraints
RUN pip install --no-cache-dir "apache-airflow==2.10.5" pyspark==3.5.3 boto3 psycopg2-binary pyarrow tzdata confluent-kafka \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.11.16 dbt-postgres==1.11.0
