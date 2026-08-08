FROM python:3.11-slim-bookworm

# Spark runs on the JVM even when using the Python API — needs a JRE.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark_streaming_job.py .

CMD ["python", "spark_streaming_job.py"]