FROM apache/airflow:2.10.4-python3.11

USER root

# PySpark/Delta Lake need a JDK
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt