# KilluHub — Spark application image
#
# Builds a self-contained image that can be submitted via Spark Operator.
# PySpark is embedded in the image so no spark-submit binary is needed
# on the submitting machine.
#
# Build:
#   docker build -t registry.example.com/killuhub:latest .
#   docker push  registry.example.com/killuhub:latest
#
# The image is used in SparkApplication CRDs as:
#   image: "registry.example.com/killuhub:latest"
#   mainApplicationFile: "local:///app/main.py"

# ── Stage 1: dependency builder ──────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# System deps needed to compile psycopg2 and confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: runtime image ───────────────────────────────────────────────────
# Use Spark's official Python image as base — it includes Java 17, Spark 3.5,
# and PySpark pre-installed. Spark Operator expects this layout.
FROM apache/spark-py:v3.5.1

USER root

# Install system libraries needed at runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy Python packages from builder
COPY --from=builder /install /usr/local

# Copy application code
WORKDIR /app
COPY killuhub/      killuhub/
COPY main.py        main.py
COPY pyproject.toml pyproject.toml

# Install the package itself (editable-style, no extra deps)
RUN pip install --no-cache-dir --no-deps -e .

# Prometheus JMX exporter jar — used by Spark Operator monitoring
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar \
    /prometheus/jmx_prometheus_javaagent.jar

# Pre-download Iceberg Spark runtime JAR to avoid network calls inside K8s
# (spark.jars.packages tries Maven at startup which may be blocked in private clusters)
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar" \
    -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar

# Use spark user (required by Spark Operator)
USER 185

ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["driver"]
