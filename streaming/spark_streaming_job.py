"""
Spark Structured Streaming job — Step 3.

Reads aircraft state events from Kafka topic `aircraft_state_v1` and:
  1) Writes each raw record to Postgres `aircraft_state` (curated feed for
     the heat map / callsign lookups in Metabase, Step 6).
  2) Computes 1-minute watermarked windowed aggregates — active flights,
     average altitude, vertical-rate anomalies — into Postgres
     `flight_window_metrics`.

Runs continuously (this is streaming, not a one-shot batch job) — the
container stays up as long as docker-compose is running.

NOTE on output modes: the windowed query uses outputMode("append"), which
only emits a window once the watermark has passed it (so each window is
written exactly once — safe for a straight JDBC append against a table with
a (window_start, window_end) primary key). This means results show up ~2
minutes after a window closes, not instantly — that's expected.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, approx_count_distinct, abs as sql_abs, when, sum as sql_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "aircraft_state_v1")

PG_URL = os.environ.get("PG_URL", "jdbc:postgresql://postgres:5432/streaming")
PG_USER = os.environ.get("PG_USER", "matrix")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "matrix")
PG_PROPERTIES = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

# |vertical_rate| above this (m/s) counts as an anomaly for the metrics table.
VERTICAL_RATE_ANOMALY_THRESHOLD = 15.0

SCHEMA = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("event_time", TimestampType()),
])


def write_to_postgres(table_name):
    """Returns a foreachBatch fn that appends a micro-batch to `table_name`.

    Structured Streaming has no built-in JDBC sink, so foreachBatch +
    the regular batch .write.jdbc() is the standard workaround.
    """
    def _write(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        batch_df.write.jdbc(url=PG_URL, table=table_name, mode="append", properties=PG_PROPERTIES)
    return _write


def main():
    spark = (
        SparkSession.builder
        .appName("aircraft-state-streaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        # "earliest" so we also process the test batch you already published
        # in Step 2, not just brand-new messages from now on.
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw
        .select(from_json(col("value").cast("string"), SCHEMA).alias("data"))
        .select("data.*")
        .withWatermark("event_time", "2 minutes")
    )

    # --- Sink 1: curated raw feed -> aircraft_state -------------------
    curated_query = (
        parsed
        .writeStream
        .foreachBatch(write_to_postgres("aircraft_state"))
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/aircraft_state")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # --- Sink 2: 1-minute windowed aggregates -> flight_window_metrics -
    windowed = (
        parsed
        .groupBy(window(col("event_time"), "1 minute"))
        .agg(
            approx_count_distinct("icao24").alias("active_flights"),
            avg("baro_altitude").alias("avg_altitude"),
            sql_sum(
                when(sql_abs(col("vertical_rate")) > VERTICAL_RATE_ANOMALY_THRESHOLD, 1).otherwise(0)
            ).alias("vertical_rate_anomalies"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "active_flights", "avg_altitude", "vertical_rate_anomalies",
        )
    )

    metrics_query = (
        windowed
        .writeStream
        .foreachBatch(write_to_postgres("flight_window_metrics"))
        .outputMode("append")  # emits each window once, after the watermark passes it
        .option("checkpointLocation", "/tmp/checkpoints/flight_window_metrics")
        .trigger(processingTime="30 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()