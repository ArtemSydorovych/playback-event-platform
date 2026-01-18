#!/usr/bin/env python3
"""
Hourly Aggregation Job
======================
Rolls up raw playback events from Iceberg into hourly statistics.

Creates/updates the `hourly_content_stats` table with aggregated metrics:
- Total views, unique users, total watch time
- Average bitrate, rebuffer events
- Device type distribution

Usage:
    spark-submit --master spark://spark-master:7077 hourly_aggregation.py [--date YYYY-MM-DD]

If no date is specified, processes the previous hour.
"""

import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, DateType


def create_spark_session():
    """Create Spark session with Iceberg configuration."""
    return (SparkSession.builder
        .appName("HourlyAggregationJob")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "iceberg")
        .getOrCreate())


def ensure_target_table_exists(spark):
    """Create the hourly stats table if it doesn't exist."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.warehouse.hourly_content_stats (
            content_id STRING,
            event_date DATE,
            event_hour INT,
            total_events BIGINT,
            play_starts BIGINT,
            play_ends BIGINT,
            unique_users BIGINT,
            unique_sessions BIGINT,
            total_watch_time_seconds BIGINT,
            avg_bitrate_kbps DOUBLE,
            rebuffer_count BIGINT,
            total_rebuffer_duration_ms BIGINT,
            quality_changes BIGINT,
            seek_count BIGINT,
            pause_count BIGINT,
            mobile_events BIGINT,
            tv_events BIGINT,
            web_events BIGINT,
            tablet_events BIGINT,
            processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (event_date)
    """)
    print("Target table 'hourly_content_stats' ensured.")


def get_processing_window(args):
    """Determine the hour to process based on arguments or default to previous hour."""
    if len(args) > 1 and args[1] == '--date':
        target_date = datetime.strptime(args[2], '%Y-%m-%d')
        target_hour = int(args[3]) if len(args) > 3 else 0
    else:
        # Default: process previous hour
        now = datetime.now()
        previous_hour = now - timedelta(hours=1)
        target_date = previous_hour.date()
        target_hour = previous_hour.hour

    return target_date, target_hour


def run_aggregation(spark, event_date, event_hour):
    """Run the hourly aggregation for a specific hour."""
    print(f"Processing aggregation for {event_date} hour {event_hour}")

    # Check if source table exists
    try:
        spark.sql("SELECT 1 FROM iceberg.warehouse.playback_events LIMIT 1")
    except Exception as e:
        print(f"Source table 'playback_events' not found or empty: {e}")
        return 0

    # Read events for the target hour
    events_df = spark.sql(f"""
        SELECT *
        FROM iceberg.warehouse.playback_events
        WHERE event_date = DATE '{event_date}'
          AND event_hour = {event_hour}
    """)

    event_count = events_df.count()
    if event_count == 0:
        print(f"No events found for {event_date} hour {event_hour}")
        return 0

    print(f"Found {event_count} events to aggregate")

    # Perform aggregation
    aggregated = events_df.groupBy("content_id").agg(
        # Event counts
        F.count("*").alias("total_events"),
        F.sum(F.when(F.col("event_type") == "PLAY_START", 1).otherwise(0)).alias("play_starts"),
        F.sum(F.when(F.col("event_type") == "PLAYBACK_END", 1).otherwise(0)).alias("play_ends"),

        # Unique counts
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("session_id").alias("unique_sessions"),

        # Watch time (from playback end events position)
        F.sum(F.when(F.col("event_type") == "PLAYBACK_END", F.col("position_ms") / 1000).otherwise(0)).cast("bigint").alias("total_watch_time_seconds"),

        # Quality metrics
        F.avg(F.col("bitrate_kbps")).alias("avg_bitrate_kbps"),
        F.sum(F.when(F.col("event_type") == "REBUFFER_START", 1).otherwise(0)).alias("rebuffer_count"),
        F.sum(F.when(F.col("event_type") == "REBUFFER_END", F.col("rebuffer_duration_ms")).otherwise(0)).cast("bigint").alias("total_rebuffer_duration_ms"),

        # User interaction
        F.sum(F.when(F.col("event_type") == "QUALITY_CHANGE", 1).otherwise(0)).alias("quality_changes"),
        F.sum(F.when(F.col("event_type") == "SEEK", 1).otherwise(0)).alias("seek_count"),
        F.sum(F.when(F.col("event_type") == "PAUSE", 1).otherwise(0)).alias("pause_count"),

        # Device distribution
        F.sum(F.when(F.col("device_type") == "MOBILE", 1).otherwise(0)).alias("mobile_events"),
        F.sum(F.when(F.col("device_type") == "TV", 1).otherwise(0)).alias("tv_events"),
        F.sum(F.when(F.col("device_type") == "WEB", 1).otherwise(0)).alias("web_events"),
        F.sum(F.when(F.col("device_type") == "TABLET", 1).otherwise(0)).alias("tablet_events"),
    ).withColumn("event_date", F.lit(event_date).cast("date")) \
     .withColumn("event_hour", F.lit(event_hour)) \
     .withColumn("processed_at", F.current_timestamp())

    # Delete existing data for this hour (idempotent)
    spark.sql(f"""
        DELETE FROM iceberg.warehouse.hourly_content_stats
        WHERE event_date = DATE '{event_date}' AND event_hour = {event_hour}
    """)

    # Write aggregated results
    aggregated.writeTo("iceberg.warehouse.hourly_content_stats").append()

    result_count = aggregated.count()
    print(f"Written {result_count} content aggregations for {event_date} hour {event_hour}")

    return result_count


def main():
    print("=" * 60)
    print("Hourly Aggregation Job - Playback Event Platform")
    print("=" * 60)

    spark = create_spark_session()

    try:
        # Ensure target table exists
        ensure_target_table_exists(spark)

        # Get processing window
        event_date, event_hour = get_processing_window(sys.argv)

        # Run aggregation
        count = run_aggregation(spark, event_date, event_hour)

        print(f"\nJob completed successfully. Aggregated {count} content records.")

    except Exception as e:
        print(f"Error during aggregation: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
