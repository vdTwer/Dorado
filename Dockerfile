# Airflow image with Java and PySpark, so tasks can run Spark in local mode.
FROM apache/airflow:3.3.2-python3.12

USER root
# Spark 4 runs on the JVM and needs Java 17 or newer.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY --chown=airflow:root pyproject.toml README.md /opt/dorado/
COPY --chown=airflow:root src /opt/dorado/src
# Airflow's constraint file pins its own dependency tree. Installing against it
# keeps our package from upgrading a library that Airflow relies on.
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    /opt/dorado \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"
