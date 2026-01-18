package com.artemsydorovych.playback.flink.job;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.flink.config.JobParameters;
import com.artemsydorovych.playback.flink.function.PlaybackEventToRowDataMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Map;

/**
 * Lakehouse Job - Writes all playback events to Apache Iceberg tables.
 *
 * Production deployment: One job per container with dedicated Kafka consumer group.
 *
 * Pipeline:
 * <pre>
 *   Kafka (playback-events)
 *          │
 *          ▼
 *   Map to RowData
 *          │
 *          ▼
 *   ┌──────────────────────────────────────┐
 *   │  Iceberg: warehouse.playback_events  │
 *   │  Partitioned by: event_date, hour    │
 *   │  Storage: s3://warehouse/            │
 *   └──────────────────────────────────────┘
 * </pre>
 *
 * Query via Trino:
 *   SELECT * FROM iceberg.warehouse.playback_events
 *   WHERE event_date = current_date;
 */
public class LakehouseJob extends AbstractPlaybackJob {

    private static final String JOB_NAME = "playback-lakehouse";
    private static final String CONSUMER_GROUP = "flink-lakehouse-consumer";

    // Iceberg configuration defaults
    private static final String DEFAULT_CATALOG_URI = "http://iceberg-rest:8181";
    private static final String DEFAULT_WAREHOUSE = "s3://warehouse/";
    private static final String DEFAULT_S3_ENDPOINT = "http://minio:9000";
    private static final String DEFAULT_AWS_REGION = "us-east-1";

    // Table configuration
    private static final String CATALOG_NAME = "playback";
    private static final String NAMESPACE = "warehouse";
    private static final String TABLE_NAME = "playback_events";

    public static void main(String[] args) throws Exception {
        new LakehouseJob().run(args);
    }

    @Override
    protected String getJobName() {
        return JOB_NAME;
    }

    @Override
    protected String getConsumerGroupId() {
        return CONSUMER_GROUP;
    }

    @Override
    protected void buildPipeline(
            StreamExecutionEnvironment env,
            DataStream<PlaybackEvent> eventStream,
            JobParameters params) {

        // Load configuration from environment (production pattern)
        IcebergConfig config = IcebergConfig.fromEnvironment();
        log.info("Configuring Iceberg sink: catalog={}, warehouse={}", config.catalogUri, config.warehousePath);

        // Build catalog properties for REST catalog
        Map<String, String> catalogProperties = buildCatalogProperties(config);

        // Hadoop configuration for S3 access
        Configuration hadoopConf = buildHadoopConfiguration(config);

        // Create catalog loader
        CatalogLoader catalogLoader = CatalogLoader.custom(
                CATALOG_NAME,
                catalogProperties,
                hadoopConf,
                "org.apache.iceberg.rest.RESTCatalog");

        // Ensure table exists (idempotent)
        ensureTableExists(catalogLoader);

        // Create table loader
        TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), TABLE_NAME);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        // Convert PlaybackEvent to RowData
        DataStream<RowData> rowDataStream = eventStream
                .map(new PlaybackEventToRowDataMapper())
                .name("Map to RowData")
                .uid("map-to-rowdata");

        // Build Iceberg sink with production settings
        FlinkSink.forRowData(rowDataStream)
                .tableLoader(tableLoader)
                .overwrite(false)
                .upsert(false)
                .append();

        log.info("Lakehouse pipeline configured: writing to iceberg.{}.{}", NAMESPACE, TABLE_NAME);
    }

    /**
     * Builds catalog properties for Iceberg REST catalog.
     */
    private Map<String, String> buildCatalogProperties(IcebergConfig config) {
        Map<String, String> props = new HashMap<>();

        // REST catalog configuration
        props.put("type", "rest");
        props.put("uri", config.catalogUri);
        props.put("warehouse", config.warehousePath);

        // S3 FileIO configuration
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", config.s3Endpoint);
        props.put("s3.path-style-access", "true");
        props.put("client.region", config.awsRegion);

        // Production tuning
        props.put("s3.delete.enabled", "true");
        props.put("s3.multipart.upload.enabled", "true");
        props.put("s3.multipart.upload.part-size-bytes", String.valueOf(32 * 1024 * 1024)); // 32MB

        return props;
    }

    /**
     * Builds Hadoop configuration for S3A filesystem.
     */
    private Configuration buildHadoopConfiguration(IcebergConfig config) {
        Configuration conf = new Configuration();

        // S3A endpoint and credentials
        conf.set("fs.s3a.endpoint", config.s3Endpoint);
        conf.set("fs.s3a.access.key", config.awsAccessKey);
        conf.set("fs.s3a.secret.key", config.awsSecretKey);
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        // Production performance tuning
        conf.set("fs.s3a.connection.maximum", "100");
        conf.set("fs.s3a.threads.max", "64");
        conf.set("fs.s3a.multipart.size", String.valueOf(32 * 1024 * 1024)); // 32MB
        conf.set("fs.s3a.fast.upload", "true");
        conf.set("fs.s3a.fast.upload.buffer", "bytebuffer");

        return conf;
    }

    /**
     * Creates the Iceberg namespace and table if they don't exist.
     * Includes retry logic for catalog availability and proper error handling.
     */
    private void ensureTableExists(CatalogLoader catalogLoader) {
        int maxRetries = 5;
        int retryDelayMs = 5000;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.info("Attempting to create/verify Iceberg table (attempt {}/{})", attempt, maxRetries);

                Catalog catalog = catalogLoader.loadCatalog();
                TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), TABLE_NAME);
                Namespace namespace = Namespace.of(NAMESPACE);

                // Create namespace if catalog supports it
                if (catalog instanceof SupportsNamespaces) {
                    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
                    try {
                        if (!nsCatalog.namespaceExists(namespace)) {
                            log.info("Creating namespace: {}", NAMESPACE);
                            nsCatalog.createNamespace(namespace);
                            log.info("Namespace created successfully: {}", NAMESPACE);
                        } else {
                            log.info("Namespace already exists: {}", NAMESPACE);
                        }
                    } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
                        log.info("Namespace already exists (concurrent creation): {}", NAMESPACE);
                    }
                } else {
                    log.warn("Catalog does not support namespaces, skipping namespace creation");
                }

                // Create table if needed
                if (!catalog.tableExists(tableId)) {
                    log.info("Creating Iceberg table: {}.{}", NAMESPACE, TABLE_NAME);

                    Schema schema = createIcebergSchema();
                    // Partition by day only (hour would be redundant since it's derived from same column)
                    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                            .day("event_timestamp", "event_date")
                            .build();

                    // Table properties for production
                    Map<String, String> tableProps = new HashMap<>();
                    tableProps.put("write.format.default", "parquet");
                    tableProps.put("write.parquet.compression-codec", "zstd");
                    tableProps.put("write.target-file-size-bytes", String.valueOf(128 * 1024 * 1024)); // 128MB
                    tableProps.put("write.metadata.delete-after-commit.enabled", "true");
                    tableProps.put("write.metadata.previous-versions-max", "10");
                    tableProps.put("history.expire.max-snapshot-age-ms", String.valueOf(7 * 24 * 60 * 60 * 1000L)); // 7 days

                    catalog.createTable(tableId, schema, partitionSpec, tableProps);
                    log.info("Iceberg table created successfully: {}.{}", NAMESPACE, TABLE_NAME);
                } else {
                    log.info("Iceberg table already exists: {}.{}", NAMESPACE, TABLE_NAME);
                }

                // Success - exit retry loop
                return;

            } catch (Exception e) {
                log.error("Failed to create/verify Iceberg table (attempt {}/{}): {}",
                        attempt, maxRetries, e.getMessage());

                if (attempt == maxRetries) {
                    throw new RuntimeException(
                            "Failed to create Iceberg table after " + maxRetries + " attempts: " + e.getMessage(), e);
                }

                try {
                    log.info("Waiting {}ms before retry...", retryDelayMs);
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting to retry table creation", ie);
                }
            }
        }
    }

    /**
     * Creates the Iceberg schema matching PlaybackEvent fields.
     */
    private Schema createIcebergSchema() {
        return new Schema(
                Types.NestedField.required(1, "event_id", Types.StringType.get()),
                Types.NestedField.required(2, "event_type", Types.StringType.get()),
                Types.NestedField.required(3, "event_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.required(4, "user_id", Types.StringType.get()),
                Types.NestedField.optional(5, "profile_id", Types.StringType.get()),
                Types.NestedField.required(6, "device_id", Types.StringType.get()),
                Types.NestedField.required(7, "session_id", Types.StringType.get()),
                Types.NestedField.required(8, "content_id", Types.StringType.get()),
                Types.NestedField.optional(9, "content_type", Types.StringType.get()),
                Types.NestedField.optional(10, "title", Types.StringType.get()),
                Types.NestedField.optional(11, "position_ms", Types.LongType.get()),
                Types.NestedField.optional(12, "duration_ms", Types.LongType.get()),
                Types.NestedField.optional(13, "device_type", Types.StringType.get()),
                Types.NestedField.optional(14, "os_name", Types.StringType.get()),
                Types.NestedField.optional(15, "os_version", Types.StringType.get()),
                Types.NestedField.optional(16, "country", Types.StringType.get()),
                Types.NestedField.optional(17, "region", Types.StringType.get()),
                Types.NestedField.optional(18, "payload_json", Types.StringType.get())
        );
    }

    /**
     * Gets the Flink RowType schema matching the Iceberg table.
     */
    public static RowType getRowType() {
        return RowType.of(
                new LogicalType[]{
                        new VarCharType(VarCharType.MAX_LENGTH),   // event_id
                        new VarCharType(VarCharType.MAX_LENGTH),   // event_type
                        new TimestampType(3),                       // event_timestamp
                        new VarCharType(VarCharType.MAX_LENGTH),   // user_id
                        new VarCharType(VarCharType.MAX_LENGTH),   // profile_id
                        new VarCharType(VarCharType.MAX_LENGTH),   // device_id
                        new VarCharType(VarCharType.MAX_LENGTH),   // session_id
                        new VarCharType(VarCharType.MAX_LENGTH),   // content_id
                        new VarCharType(VarCharType.MAX_LENGTH),   // content_type
                        new VarCharType(VarCharType.MAX_LENGTH),   // title
                        new BigIntType(),                           // position_ms
                        new BigIntType(),                           // duration_ms
                        new VarCharType(VarCharType.MAX_LENGTH),   // device_type
                        new VarCharType(VarCharType.MAX_LENGTH),   // os_name
                        new VarCharType(VarCharType.MAX_LENGTH),   // os_version
                        new VarCharType(VarCharType.MAX_LENGTH),   // country
                        new VarCharType(VarCharType.MAX_LENGTH),   // region
                        new VarCharType(VarCharType.MAX_LENGTH)    // payload_json
                },
                new String[]{
                        "event_id", "event_type", "event_timestamp",
                        "user_id", "profile_id", "device_id", "session_id",
                        "content_id", "content_type", "title",
                        "position_ms", "duration_ms",
                        "device_type", "os_name", "os_version", "country", "region",
                        "payload_json"
                }
        );
    }

    /**
     * Configuration holder for Iceberg settings (loaded from environment).
     * Production pattern: all configuration externalized via environment variables.
     */
    private static class IcebergConfig {
        final String catalogUri;
        final String warehousePath;
        final String s3Endpoint;
        final String awsAccessKey;
        final String awsSecretKey;
        final String awsRegion;

        private IcebergConfig(String catalogUri, String warehousePath, String s3Endpoint,
                              String awsAccessKey, String awsSecretKey, String awsRegion) {
            this.catalogUri = catalogUri;
            this.warehousePath = warehousePath;
            this.s3Endpoint = s3Endpoint;
            this.awsAccessKey = awsAccessKey;
            this.awsSecretKey = awsSecretKey;
            this.awsRegion = awsRegion;
        }

        static IcebergConfig fromEnvironment() {
            return new IcebergConfig(
                    getEnv("ICEBERG_CATALOG_URI", DEFAULT_CATALOG_URI),
                    getEnv("ICEBERG_WAREHOUSE", DEFAULT_WAREHOUSE),
                    getEnv("S3_ENDPOINT", DEFAULT_S3_ENDPOINT),
                    getEnv("AWS_ACCESS_KEY_ID", "minioadmin"),
                    getEnv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
                    getEnv("AWS_REGION", DEFAULT_AWS_REGION)
            );
        }

        private static String getEnv(String key, String defaultValue) {
            String value = System.getenv(key);
            return value != null && !value.isEmpty() ? value : defaultValue;
        }
    }
}
