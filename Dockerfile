FROM apache/airflow:2.8.2-python3.10

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

RUN pip install --user --no-cache-dir "apache-airflow==2.8.2" apache-airflow-providers-apache-spark