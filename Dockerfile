FROM apache/airflow:2.10.3-python3.9
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
RUN pip install --no-cache-dir duckdb pyarrow pyiceberg[s3fs,duckdb,hive,pyarrow] minio apache-airflow-providers-mongo pymongo pyspark==3.5.1 apache-airflow-providers-apache-spark lxml
RUN pip install dbt-core==1.8.9 dbt-duckdb==1.9.1
