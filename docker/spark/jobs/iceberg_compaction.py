#!/usr/bin/env python3
"""
Iceberg Compaction Job
======================
Performs table maintenance on Iceberg tables to optimize query performance.

Operations:
1. Rewrite data files - Compact small files into larger ones
2. Rewrite manifests - Optimize metadata file layout
3. Expire snapshots - Remove old snapshots to reclaim storage
4. Remove orphan files - Clean up files not referenced by metadata

Usage:
    spark-submit --master spark://spark-master:7077 iceberg_compaction.py [--table TABLE_NAME]

If no table is specified, maintains all tables in the warehouse namespace.
"""

import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


def create_spark_session():
    """Create Spark session with Iceberg configuration."""
    return (SparkSession.builder
        .appName("IcebergCompactionJob")
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


def get_tables_to_maintain(spark, specific_table=None):
    """Get list of tables to maintain."""
    if specific_table:
        return [f"iceberg.warehouse.{specific_table}"]

    # List all tables in warehouse namespace
    try:
        tables_df = spark.sql("SHOW TABLES IN iceberg.warehouse")
        tables = [f"iceberg.warehouse.{row.tableName}" for row in tables_df.collect()]
        return tables
    except Exception as e:
        print(f"Error listing tables: {e}")
        return []


def get_table_stats(spark, table_name):
    """Get statistics about a table."""
    try:
        # Count files
        files_df = spark.sql(f"SELECT * FROM {table_name}.files")
        file_count = files_df.count()
        total_size_mb = files_df.agg({"file_size_in_bytes": "sum"}).collect()[0][0] or 0
        total_size_mb = total_size_mb / (1024 * 1024)

        # Count records
        record_count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]

        # Count snapshots
        snapshots_df = spark.sql(f"SELECT * FROM {table_name}.snapshots")
        snapshot_count = snapshots_df.count()

        return {
            "file_count": file_count,
            "total_size_mb": round(total_size_mb, 2),
            "record_count": record_count,
            "snapshot_count": snapshot_count
        }
    except Exception as e:
        print(f"Error getting stats for {table_name}: {e}")
        return None


def compact_data_files(spark, table_name):
    """Rewrite small data files into larger ones."""
    print(f"\n  Compacting data files...")

    try:
        # Use Iceberg's rewrite_data_files procedure
        # Target file size: 128MB (default Iceberg file size)
        result = spark.sql(f"""
            CALL iceberg.system.rewrite_data_files(
                table => '{table_name}',
                options => map(
                    'target-file-size-bytes', '134217728',
                    'min-input-files', '2',
                    'partial-progress.enabled', 'true'
                )
            )
        """)

        row = result.collect()[0]
        rewritten = row.rewritten_data_files_count
        added = row.added_data_files_count
        print(f"  Rewrote {rewritten} files into {added} files")
        return True

    except Exception as e:
        if "no matching data files" in str(e).lower() or "nothing to compact" in str(e).lower():
            print("  No files need compaction")
            return True
        print(f"  Error compacting files: {e}")
        return False


def rewrite_manifests(spark, table_name):
    """Optimize manifest files for better query planning."""
    print(f"\n  Rewriting manifests...")

    try:
        result = spark.sql(f"""
            CALL iceberg.system.rewrite_manifests(
                table => '{table_name}'
            )
        """)

        row = result.collect()[0]
        rewritten = row.rewritten_manifests_count
        added = row.added_manifests_count
        print(f"  Rewrote {rewritten} manifests into {added} manifests")
        return True

    except Exception as e:
        if "no matching manifests" in str(e).lower():
            print("  No manifests need rewriting")
            return True
        print(f"  Error rewriting manifests: {e}")
        return False


def expire_snapshots(spark, table_name, retention_days=7):
    """Expire old snapshots to reclaim storage."""
    print(f"\n  Expiring snapshots older than {retention_days} days...")

    try:
        # Calculate timestamp for retention
        retention_ts = datetime.now() - timedelta(days=retention_days)
        retention_ms = int(retention_ts.timestamp() * 1000)

        result = spark.sql(f"""
            CALL iceberg.system.expire_snapshots(
                table => '{table_name}',
                older_than => TIMESTAMP '{retention_ts.strftime("%Y-%m-%d %H:%M:%S")}',
                retain_last => 5
            )
        """)

        row = result.collect()[0]
        deleted_files = row.deleted_data_files_count
        deleted_manifests = row.deleted_manifest_files_count
        print(f"  Expired snapshots, deleted {deleted_files} data files and {deleted_manifests} manifests")
        return True

    except Exception as e:
        print(f"  Error expiring snapshots: {e}")
        return False


def remove_orphan_files(spark, table_name, retention_days=3):
    """Remove orphan files not referenced by any snapshot."""
    print(f"\n  Removing orphan files older than {retention_days} days...")

    try:
        retention_ts = datetime.now() - timedelta(days=retention_days)

        result = spark.sql(f"""
            CALL iceberg.system.remove_orphan_files(
                table => '{table_name}',
                older_than => TIMESTAMP '{retention_ts.strftime("%Y-%m-%d %H:%M:%S")}'
            )
        """)

        orphan_files = result.collect()
        print(f"  Removed {len(orphan_files)} orphan files")
        return True

    except Exception as e:
        print(f"  Error removing orphan files: {e}")
        return False


def maintain_table(spark, table_name):
    """Run all maintenance operations on a table."""
    print(f"\n{'='*60}")
    print(f"Maintaining table: {table_name}")
    print("=" * 60)

    # Get before stats
    before_stats = get_table_stats(spark, table_name)
    if before_stats:
        print(f"\nBefore maintenance:")
        print(f"  Files: {before_stats['file_count']}, Size: {before_stats['total_size_mb']} MB")
        print(f"  Records: {before_stats['record_count']}, Snapshots: {before_stats['snapshot_count']}")

    # Run maintenance operations
    compact_data_files(spark, table_name)
    rewrite_manifests(spark, table_name)
    expire_snapshots(spark, table_name)
    remove_orphan_files(spark, table_name)

    # Get after stats
    after_stats = get_table_stats(spark, table_name)
    if after_stats:
        print(f"\nAfter maintenance:")
        print(f"  Files: {after_stats['file_count']}, Size: {after_stats['total_size_mb']} MB")
        print(f"  Records: {after_stats['record_count']}, Snapshots: {after_stats['snapshot_count']}")

    return True


def main():
    print("=" * 60)
    print("Iceberg Compaction Job - Playback Event Platform")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")

    spark = create_spark_session()

    try:
        # Parse arguments
        specific_table = None
        if len(sys.argv) > 1 and sys.argv[1] == '--table':
            specific_table = sys.argv[2]

        # Get tables to maintain
        tables = get_tables_to_maintain(spark, specific_table)

        if not tables:
            print("No tables found to maintain")
            return

        print(f"\nTables to maintain: {len(tables)}")
        for table in tables:
            print(f"  - {table}")

        # Maintain each table
        success_count = 0
        for table in tables:
            try:
                if maintain_table(spark, table):
                    success_count += 1
            except Exception as e:
                print(f"Failed to maintain {table}: {e}")

        print(f"\n{'='*60}")
        print(f"Compaction completed: {success_count}/{len(tables)} tables maintained")
        print(f"Finished at: {datetime.now()}")

    except Exception as e:
        print(f"Error during compaction: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
