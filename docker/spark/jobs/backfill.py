#!/usr/bin/env python3
"""
Backfill Job
============
Reprocesses historical data from the Iceberg data lake to regenerate
aggregated tables. Useful for:
- Fixing data quality issues
- Regenerating after schema changes
- Adding new computed columns

Usage:
    spark-submit --master spark://spark-master:7077 backfill.py \
        --start-date YYYY-MM-DD \
        --end-date YYYY-MM-DD \
        [--target hourly_content_stats|daily_content_stats|all]

Examples:
    # Backfill last 7 days of hourly stats
    spark-submit backfill.py --start-date 2026-01-11 --end-date 2026-01-18

    # Backfill specific table
    spark-submit backfill.py --start-date 2026-01-01 --end-date 2026-01-31 --target hourly_content_stats
"""

import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    """Create Spark session with Iceberg configuration."""
    return (SparkSession.builder
        .appName("BackfillJob")
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


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Backfill historical data')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--target', default='all',
                        choices=['hourly_content_stats', 'daily_content_stats', 'all'],
                        help='Target table to backfill')
    return parser.parse_args()


def ensure_hourly_table_exists(spark):
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


def ensure_daily_table_exists(spark):
    """Create the daily stats table if it doesn't exist."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.warehouse.daily_content_stats (
            content_id STRING,
            event_date DATE,
            total_events BIGINT,
            play_starts BIGINT,
            play_ends BIGINT,
            unique_users BIGINT,
            unique_sessions BIGINT,
            total_watch_time_seconds BIGINT,
            avg_bitrate_kbps DOUBLE,
            total_rebuffer_count BIGINT,
            total_rebuffer_duration_ms BIGINT,
            quality_changes BIGINT,
            seek_count BIGINT,
            pause_count BIGINT,
            peak_hour INT,
            peak_hour_events BIGINT,
            mobile_ratio DOUBLE,
            tv_ratio DOUBLE,
            web_ratio DOUBLE,
            tablet_ratio DOUBLE,
            processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (event_date)
    """)


def backfill_hourly_stats(spark, start_date, end_date):
    """Backfill hourly content statistics."""
    print(f"\n{'='*60}")
    print(f"Backfilling hourly_content_stats: {start_date} to {end_date}")
    print("=" * 60)

    ensure_hourly_table_exists(spark)

    # Check source table
    try:
        source_count = spark.sql(f"""
            SELECT COUNT(*) FROM iceberg.warehouse.playback_events
            WHERE event_date >= DATE '{start_date}' AND event_date <= DATE '{end_date}'
        """).collect()[0][0]
        print(f"Source events in range: {source_count}")
    except Exception as e:
        print(f"Error accessing source table: {e}")
        return 0

    if source_count == 0:
        print("No source data to process")
        return 0

    # Delete existing data in range (idempotent backfill)
    print("Deleting existing data in range...")
    spark.sql(f"""
        DELETE FROM iceberg.warehouse.hourly_content_stats
        WHERE event_date >= DATE '{start_date}' AND event_date <= DATE '{end_date}'
    """)

    # Read source data
    print("Reading source events...")
    events_df = spark.sql(f"""
        SELECT *
        FROM iceberg.warehouse.playback_events
        WHERE event_date >= DATE '{start_date}' AND event_date <= DATE '{end_date}'
    """)

    # Aggregate by content_id, date, and hour
    print("Aggregating hourly statistics...")
    aggregated = events_df.groupBy("content_id", "event_date", "event_hour").agg(
        F.count("*").alias("total_events"),
        F.sum(F.when(F.col("event_type") == "PLAY_START", 1).otherwise(0)).alias("play_starts"),
        F.sum(F.when(F.col("event_type") == "PLAYBACK_END", 1).otherwise(0)).alias("play_ends"),
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("session_id").alias("unique_sessions"),
        F.sum(F.when(F.col("event_type") == "PLAYBACK_END", F.col("position_ms") / 1000).otherwise(0)).cast("bigint").alias("total_watch_time_seconds"),
        F.avg(F.col("bitrate_kbps")).alias("avg_bitrate_kbps"),
        F.sum(F.when(F.col("event_type") == "REBUFFER_START", 1).otherwise(0)).alias("rebuffer_count"),
        F.sum(F.when(F.col("event_type") == "REBUFFER_END", F.col("rebuffer_duration_ms")).otherwise(0)).cast("bigint").alias("total_rebuffer_duration_ms"),
        F.sum(F.when(F.col("event_type") == "QUALITY_CHANGE", 1).otherwise(0)).alias("quality_changes"),
        F.sum(F.when(F.col("event_type") == "SEEK", 1).otherwise(0)).alias("seek_count"),
        F.sum(F.when(F.col("event_type") == "PAUSE", 1).otherwise(0)).alias("pause_count"),
        F.sum(F.when(F.col("device_type") == "MOBILE", 1).otherwise(0)).alias("mobile_events"),
        F.sum(F.when(F.col("device_type") == "TV", 1).otherwise(0)).alias("tv_events"),
        F.sum(F.when(F.col("device_type") == "WEB", 1).otherwise(0)).alias("web_events"),
        F.sum(F.when(F.col("device_type") == "TABLET", 1).otherwise(0)).alias("tablet_events"),
    ).withColumn("processed_at", F.current_timestamp())

    # Write results
    print("Writing hourly statistics...")
    aggregated.writeTo("iceberg.warehouse.hourly_content_stats").append()

    result_count = aggregated.count()
    print(f"Written {result_count} hourly aggregation records")

    return result_count


def backfill_daily_stats(spark, start_date, end_date):
    """Backfill daily content statistics from hourly stats."""
    print(f"\n{'='*60}")
    print(f"Backfilling daily_content_stats: {start_date} to {end_date}")
    print("=" * 60)

    ensure_daily_table_exists(spark)

    # First ensure hourly stats exist
    try:
        hourly_count = spark.sql(f"""
            SELECT COUNT(*) FROM iceberg.warehouse.hourly_content_stats
            WHERE event_date >= DATE '{start_date}' AND event_date <= DATE '{end_date}'
        """).collect()[0][0]
        print(f"Hourly stats in range: {hourly_count}")
    except Exception as e:
        print(f"Error accessing hourly stats: {e}")
        return 0

    if hourly_count == 0:
        print("No hourly data to aggregate into daily stats")
        return 0

    # Delete existing data in range
    print("Deleting existing data in range...")
    spark.sql(f"""
        DELETE FROM iceberg.warehouse.daily_content_stats
        WHERE event_date >= DATE '{start_date}' AND event_date <= DATE '{end_date}'
    """)

    # Read hourly data
    print("Reading hourly statistics...")
    hourly_df = spark.sql(f"""
        SELECT *
        FROM iceberg.warehouse.hourly_content_stats
        WHERE event_date >= DATE '{start_date}' AND event_date <= DATE '{end_date}'
    """)

    # Find peak hour per content per day
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("content_id", "event_date").orderBy(F.desc("total_events"))

    hourly_with_rank = hourly_df.withColumn("rank", F.row_number().over(window_spec))
    peak_hours = hourly_with_rank.filter(F.col("rank") == 1).select(
        "content_id", "event_date",
        F.col("event_hour").alias("peak_hour"),
        F.col("total_events").alias("peak_hour_events")
    )

    # Aggregate to daily
    print("Aggregating daily statistics...")
    daily_agg = hourly_df.groupBy("content_id", "event_date").agg(
        F.sum("total_events").alias("total_events"),
        F.sum("play_starts").alias("play_starts"),
        F.sum("play_ends").alias("play_ends"),
        F.max("unique_users").alias("unique_users"),  # Approximation
        F.max("unique_sessions").alias("unique_sessions"),  # Approximation
        F.sum("total_watch_time_seconds").alias("total_watch_time_seconds"),
        F.avg("avg_bitrate_kbps").alias("avg_bitrate_kbps"),
        F.sum("rebuffer_count").alias("total_rebuffer_count"),
        F.sum("total_rebuffer_duration_ms").alias("total_rebuffer_duration_ms"),
        F.sum("quality_changes").alias("quality_changes"),
        F.sum("seek_count").alias("seek_count"),
        F.sum("pause_count").alias("pause_count"),
        F.sum("mobile_events").alias("mobile_total"),
        F.sum("tv_events").alias("tv_total"),
        F.sum("web_events").alias("web_total"),
        F.sum("tablet_events").alias("tablet_total"),
    )

    # Calculate device ratios
    daily_agg = daily_agg.withColumn(
        "device_total",
        F.col("mobile_total") + F.col("tv_total") + F.col("web_total") + F.col("tablet_total")
    ).withColumn(
        "mobile_ratio",
        F.when(F.col("device_total") > 0, F.col("mobile_total") / F.col("device_total")).otherwise(0)
    ).withColumn(
        "tv_ratio",
        F.when(F.col("device_total") > 0, F.col("tv_total") / F.col("device_total")).otherwise(0)
    ).withColumn(
        "web_ratio",
        F.when(F.col("device_total") > 0, F.col("web_total") / F.col("device_total")).otherwise(0)
    ).withColumn(
        "tablet_ratio",
        F.when(F.col("device_total") > 0, F.col("tablet_total") / F.col("device_total")).otherwise(0)
    ).drop("mobile_total", "tv_total", "web_total", "tablet_total", "device_total")

    # Join with peak hours
    daily_stats = daily_agg.join(peak_hours, ["content_id", "event_date"], "left") \
        .withColumn("processed_at", F.current_timestamp())

    # Write results
    print("Writing daily statistics...")
    daily_stats.writeTo("iceberg.warehouse.daily_content_stats").append()

    result_count = daily_stats.count()
    print(f"Written {result_count} daily aggregation records")

    return result_count


def main():
    args = parse_args()

    print("=" * 60)
    print("Backfill Job - Playback Event Platform")
    print("=" * 60)
    print(f"Start Date: {args.start_date}")
    print(f"End Date: {args.end_date}")
    print(f"Target: {args.target}")
    print(f"Started at: {datetime.now()}")

    spark = create_spark_session()

    try:
        results = {}

        if args.target in ['hourly_content_stats', 'all']:
            results['hourly'] = backfill_hourly_stats(spark, args.start_date, args.end_date)

        if args.target in ['daily_content_stats', 'all']:
            results['daily'] = backfill_daily_stats(spark, args.start_date, args.end_date)

        print(f"\n{'='*60}")
        print("Backfill Summary")
        print("=" * 60)
        for table, count in results.items():
            print(f"  {table}: {count} records")
        print(f"Finished at: {datetime.now()}")

    except Exception as e:
        print(f"Error during backfill: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
