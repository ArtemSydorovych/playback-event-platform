package com.artemsydorovych.playback.flink.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

/**
 * Central registry for custom Prometheus metrics in playback processing.
 *
 * Metric naming follows Prometheus conventions:
 * - Snake_case names
 * - Suffix indicates type (_total for counters, _seconds for durations)
 * - Labels via metric groups
 *
 * These metrics are automatically exported via Flink's Prometheus reporter.
 */
public class PlaybackMetrics {

    private static final int HISTOGRAM_WINDOW_SIZE = 500;

    // ==========================================================================
    // EVENT PROCESSING METRICS
    // ==========================================================================

    /**
     * Creates event processing metrics for a source or operator.
     */
    public static EventMetrics createEventMetrics(MetricGroup metricGroup, String jobName) {
        MetricGroup group = metricGroup
                .addGroup("playback")
                .addGroup("events")
                .addGroup("job", jobName);
        return new EventMetrics(group);
    }

    /**
     * Metrics for event processing rates and types.
     */
    public static class EventMetrics {
        public final Counter eventsProcessedTotal;
        public final Counter playStartsTotal;
        public final Counter playbackEndsTotal;
        public final Counter pausesTotal;
        public final Counter seeksTotal;
        public final Counter rebuffersTotal;
        public final Counter qualityChangesTotal;
        public final Counter errorsTotal;
        public final Histogram processingLatencyMs;
        public final MeterView eventsPerSecond;

        public EventMetrics(MetricGroup group) {
            this.eventsProcessedTotal = group.counter("events_processed_total");
            this.playStartsTotal = group.counter("play_starts_total");
            this.playbackEndsTotal = group.counter("playback_ends_total");
            this.pausesTotal = group.counter("pauses_total");
            this.seeksTotal = group.counter("seeks_total");
            this.rebuffersTotal = group.counter("rebuffers_total");
            this.qualityChangesTotal = group.counter("quality_changes_total");
            this.errorsTotal = group.counter("errors_total");
            this.processingLatencyMs = group.histogram("processing_latency_ms",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
            this.eventsPerSecond = group.meter("events_per_second", new MeterView(eventsProcessedTotal));
        }

        public void recordEventType(String eventType) {
            eventsProcessedTotal.inc();
            switch (eventType) {
                case "PLAY_START" -> playStartsTotal.inc();
                case "PLAYBACK_END" -> playbackEndsTotal.inc();
                case "PAUSE" -> pausesTotal.inc();
                case "SEEK" -> seeksTotal.inc();
                case "REBUFFER_END", "REBUFFER_START" -> rebuffersTotal.inc();
                case "QUALITY_CHANGE" -> qualityChangesTotal.inc();
                case "ERROR" -> errorsTotal.inc();
            }
        }
    }

    // ==========================================================================
    // CASSANDRA SINK METRICS
    // ==========================================================================

    /**
     * Creates Cassandra sink metrics.
     */
    public static CassandraMetrics createCassandraMetrics(MetricGroup metricGroup, String tableName) {
        MetricGroup group = metricGroup
                .addGroup("playback")
                .addGroup("cassandra")
                .addGroup("table", tableName);
        return new CassandraMetrics(group);
    }

    /**
     * Metrics for Cassandra write operations.
     */
    public static class CassandraMetrics {
        public final Counter writesTotal;
        public final Counter writeErrorsTotal;
        public final Counter writeBatchesTotal;
        public final Histogram writeLatencyMs;
        public final Histogram batchSize;
        public final MeterView writesPerSecond;

        private long currentBatchSize = 0;

        public CassandraMetrics(MetricGroup group) {
            this.writesTotal = group.counter("writes_total");
            this.writeErrorsTotal = group.counter("write_errors_total");
            this.writeBatchesTotal = group.counter("write_batches_total");
            this.writeLatencyMs = group.histogram("write_latency_ms",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
            this.batchSize = group.histogram("batch_size",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
            this.writesPerSecond = group.meter("writes_per_second", new MeterView(writesTotal));
        }

        public void recordWrite(long latencyMs) {
            writesTotal.inc();
            writeLatencyMs.update(latencyMs);
        }

        public void recordError() {
            writeErrorsTotal.inc();
        }

        public void recordBatch(int size) {
            writeBatchesTotal.inc();
            batchSize.update(size);
        }
    }

    // ==========================================================================
    // SESSION DETECTION METRICS
    // ==========================================================================

    /**
     * Creates session detection metrics.
     */
    public static SessionMetrics createSessionMetrics(MetricGroup metricGroup) {
        MetricGroup group = metricGroup
                .addGroup("playback")
                .addGroup("sessions");
        return new SessionMetrics(group);
    }

    /**
     * Metrics for session detection and tracking.
     */
    public static class SessionMetrics {
        public final Counter sessionsDetectedTotal;
        public final Counter sessionEventsTotal;
        public final Histogram sessionDurationSeconds;
        public final Histogram eventsPerSession;
        public final Histogram contentsPerSession;
        public final Histogram watchTimePerSessionSeconds;

        private long activeSessions = 0;

        public SessionMetrics(MetricGroup group) {
            this.sessionsDetectedTotal = group.counter("sessions_detected_total");
            this.sessionEventsTotal = group.counter("session_events_total");
            this.sessionDurationSeconds = group.histogram("session_duration_seconds",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
            this.eventsPerSession = group.histogram("events_per_session",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
            this.contentsPerSession = group.histogram("contents_per_session",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
            this.watchTimePerSessionSeconds = group.histogram("watch_time_per_session_seconds",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));

            // Active sessions gauge
            group.gauge("active_sessions", () -> activeSessions);
        }

        public void recordSessionStarted() {
            activeSessions++;
        }

        public void recordSessionEnded(long durationMs, int eventCount, int contentCount, long watchTimeMs) {
            activeSessions = Math.max(0, activeSessions - 1);
            sessionsDetectedTotal.inc();
            sessionDurationSeconds.update(durationMs / 1000);
            eventsPerSession.update(eventCount);
            contentsPerSession.update(contentCount);
            watchTimePerSessionSeconds.update(watchTimeMs / 1000);
        }

        public void recordSessionEvent() {
            sessionEventsTotal.inc();
        }
    }

    // ==========================================================================
    // CONTENT METRICS AGGREGATION
    // ==========================================================================

    /**
     * Creates content metrics aggregation metrics.
     */
    public static ContentAggregationMetrics createContentMetrics(MetricGroup metricGroup) {
        MetricGroup group = metricGroup
                .addGroup("playback")
                .addGroup("content_metrics");
        return new ContentAggregationMetrics(group);
    }

    /**
     * Metrics for content metrics windows.
     */
    public static class ContentAggregationMetrics {
        public final Counter windowsProcessedTotal;
        public final Counter uniqueContentsTotal;
        public final Histogram viewsPerWindow;
        public final Histogram uniqueViewersPerWindow;
        public final Histogram watchTimePerWindowMinutes;
        public final Histogram completionRatePercent;

        public ContentAggregationMetrics(MetricGroup group) {
            this.windowsProcessedTotal = group.counter("windows_processed_total");
            this.uniqueContentsTotal = group.counter("unique_contents_total");
            this.viewsPerWindow = group.histogram("views_per_window",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
            this.uniqueViewersPerWindow = group.histogram("unique_viewers_per_window",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
            this.watchTimePerWindowMinutes = group.histogram("watch_time_per_window_minutes",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
            this.completionRatePercent = group.histogram("completion_rate_percent",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
        }

        public void recordWindow(int views, int uniqueViewers, long watchTimeMs, int completedViews) {
            windowsProcessedTotal.inc();
            viewsPerWindow.update(views);
            uniqueViewersPerWindow.update(uniqueViewers);
            watchTimePerWindowMinutes.update(watchTimeMs / 60000);
            if (views > 0) {
                completionRatePercent.update((completedViews * 100) / views);
            }
        }
    }

    // ==========================================================================
    // KAFKA SOURCE METRICS
    // ==========================================================================

    /**
     * Creates Kafka source metrics.
     */
    public static KafkaMetrics createKafkaMetrics(MetricGroup metricGroup, String topic) {
        MetricGroup group = metricGroup
                .addGroup("playback")
                .addGroup("kafka")
                .addGroup("topic", topic);
        return new KafkaMetrics(group);
    }

    /**
     * Metrics for Kafka consumption.
     */
    public static class KafkaMetrics {
        public final Counter recordsConsumedTotal;
        public final Counter deserializationErrorsTotal;
        public final Histogram recordSizeBytes;
        public final MeterView recordsPerSecond;

        public KafkaMetrics(MetricGroup group) {
            this.recordsConsumedTotal = group.counter("records_consumed_total");
            this.deserializationErrorsTotal = group.counter("deserialization_errors_total");
            this.recordSizeBytes = group.histogram("record_size_bytes",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
            this.recordsPerSecond = group.meter("records_per_second", new MeterView(recordsConsumedTotal));
        }

        public void recordConsumed(int sizeBytes) {
            recordsConsumedTotal.inc();
            recordSizeBytes.update(sizeBytes);
        }

        public void recordDeserializationError() {
            deserializationErrorsTotal.inc();
        }
    }

    // ==========================================================================
    // BUSINESS METRICS
    // ==========================================================================

    /**
     * Creates business-level metrics for playback platform.
     */
    public static BusinessMetrics createBusinessMetrics(MetricGroup metricGroup) {
        MetricGroup group = metricGroup
                .addGroup("playback")
                .addGroup("business");
        return new BusinessMetrics(group);
    }

    /**
     * High-level business metrics.
     */
    public static class BusinessMetrics {
        public final Counter totalPlaybacksStarted;
        public final Counter totalPlaybacksCompleted;
        public final Counter totalWatchTimeSeconds;
        public final Counter totalRebufferTimeMs;
        public final Histogram completionPercentage;
        public final Histogram engagementScorePercent;

        public BusinessMetrics(MetricGroup group) {
            this.totalPlaybacksStarted = group.counter("total_playbacks_started");
            this.totalPlaybacksCompleted = group.counter("total_playbacks_completed");
            this.totalWatchTimeSeconds = group.counter("total_watch_time_seconds");
            this.totalRebufferTimeMs = group.counter("total_rebuffer_time_ms");
            this.completionPercentage = group.histogram("completion_percentage",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
            this.engagementScorePercent = group.histogram("engagement_score_percent",
                    new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
        }

        public void recordPlaybackStart() {
            totalPlaybacksStarted.inc();
        }

        public void recordPlaybackEnd(long watchTimeMs, double completionPct) {
            totalWatchTimeSeconds.inc(watchTimeMs / 1000);
            completionPercentage.update((long) completionPct);
            if (completionPct >= 90) {
                totalPlaybacksCompleted.inc();
            }
        }

        public void recordRebuffer(long durationMs) {
            totalRebufferTimeMs.inc(durationMs);
        }
    }
}
