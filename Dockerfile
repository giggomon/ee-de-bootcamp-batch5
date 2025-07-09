# Base image
FROM apache/airflow:2.11.0

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAG files
COPY airflow/dags/ /opt/airflow/dags/