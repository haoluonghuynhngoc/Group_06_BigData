FROM python:3.11-slim-bookworm

# Spark needs a JVM; procps provides `ps`, which Spark's launch scripts call.
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/work \
    BOOKPULSE_HOME=/work

WORKDIR /work
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Pre-fetch the Spark-Kafka connector into the Ivy cache at build time, so the stream job starts without Internet
# (the ivy-cache volume is seeded from this directory when it is first created).
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[1]')\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3').getOrCreate().stop()"
