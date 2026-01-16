package com.artemsydorovych.playback.flink.function;

import com.artemsydorovych.playback.avro.DeviceType;
import com.artemsydorovych.playback.avro.EventType;
import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.avro.PlaybackEndPayload;
import com.artemsydorovych.playback.avro.RebufferPayload;
import com.artemsydorovych.playback.flink.metrics.PlaybackMetrics;
import com.artemsydorovych.playback.flink.schema.ContentMetrics;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * Aggregates PlaybackEvents into ContentMetrics within a time window.
 * Used with tumbling windows for content analytics.
 *
 * Exports Prometheus metrics:
 * - playback_content_metrics_windows_processed_total
 * - playback_content_metrics_views_per_window
 * - playback_content_metrics_unique_viewers_per_window
 * - playback_content_metrics_watch_time_per_window_minutes
 * - playback_content_metrics_completion_rate_percent
 */
public class ContentMetricsAggregator {

    private ContentMetricsAggregator() {}

    /**
     * Returns the aggregate function for incremental aggregation.
     */
    public static AggregateFunction<PlaybackEvent, ContentMetrics, ContentMetrics> aggregateFunction() {
        return new ContentMetricsAggregateFunction();
    }

    /**
     * Returns the process window function to add window metadata.
     */
    public static ProcessWindowFunction<ContentMetrics, ContentMetrics, String, TimeWindow> windowFunction() {
        return new ContentMetricsWindowFunction();
    }

    /**
     * Incremental aggregate function that builds ContentMetrics from events.
     */
    private static class ContentMetricsAggregateFunction
            implements AggregateFunction<PlaybackEvent, ContentMetrics, ContentMetrics> {

        private static final long serialVersionUID = 1L;

        @Override
        public ContentMetrics createAccumulator() {
            return new ContentMetrics();
        }

        @Override
        public ContentMetrics add(PlaybackEvent event, ContentMetrics acc) {
            // Set contentId if not set
            if (acc.getContentId() == null) {
                acc.setContentId(event.getContentId().toString());
            }

            // Track unique viewers
            acc.addViewer(event.getUserId().toString());

            // Process based on event type
            EventType eventType = event.getEventType();

            switch (eventType) {
                case PLAY_START -> {
                    acc.incrementViewCount();
                    incrementDeviceCounter(event, acc);
                }
                case PAUSE -> acc.incrementPauseCount();
                case SEEK -> acc.incrementSeekCount();
                case QUALITY_CHANGE -> acc.incrementQualityChangeCount();
                case REBUFFER_END -> {
                    acc.incrementRebufferCount();
                    if (event.getPayload() instanceof RebufferPayload rebuffer) {
                        Long duration = rebuffer.getRebufferDuration();
                        if (duration != null) {
                            acc.addRebufferDuration(duration);
                        }
                    }
                }
                case PLAYBACK_END -> {
                    if (event.getPayload() instanceof PlaybackEndPayload endPayload) {
                        long watchTime = endPayload.getTotalWatchTime();
                        acc.addWatchTime(watchTime);
                        double completion = endPayload.getCompletionPercentage();
                        if (completion >= 90.0) {
                            acc.incrementCompletedViews();
                        }
                    }
                }
                default -> {
                    // Other events (RESUME, PROGRESS, REBUFFER_START) don't need special handling
                }
            }

            return acc;
        }

        private void incrementDeviceCounter(PlaybackEvent event, ContentMetrics acc) {
            if (event.getDeviceInfo() == null) {
                acc.incrementOtherDeviceViews();
                return;
            }

            DeviceType deviceType = event.getDeviceInfo().getDeviceType();
            if (deviceType == null) {
                acc.incrementOtherDeviceViews();
                return;
            }

            switch (deviceType) {
                case IOS, ANDROID -> acc.incrementMobileViews();
                case WEB -> acc.incrementDesktopViews();
                case SMART_TV, SET_TOP_BOX, GAMING_CONSOLE -> acc.incrementTvViews();
                default -> acc.incrementOtherDeviceViews();
            }
        }

        @Override
        public ContentMetrics getResult(ContentMetrics acc) {
            return acc;
        }

        @Override
        public ContentMetrics merge(ContentMetrics a, ContentMetrics b) {
            return a.merge(b);
        }
    }

    /**
     * Window function that adds window start/end times to the metrics.
     * Also exports Prometheus metrics for each window processed.
     */
    private static class ContentMetricsWindowFunction
            extends ProcessWindowFunction<ContentMetrics, ContentMetrics, String, TimeWindow> {

        private static final long serialVersionUID = 1L;
        private transient PlaybackMetrics.ContentAggregationMetrics contentMetrics;

        @Override
        public void open(Configuration parameters) throws Exception {
            contentMetrics = PlaybackMetrics.createContentMetrics(
                    getRuntimeContext().getMetricGroup());
        }

        @Override
        public void process(
                String contentId,
                Context context,
                Iterable<ContentMetrics> elements,
                Collector<ContentMetrics> out) {

            ContentMetrics metrics = elements.iterator().next();

            // Add window boundaries
            metrics.setContentId(contentId);
            metrics.setWindowStart(Instant.ofEpochMilli(context.window().getStart()));
            metrics.setWindowEnd(Instant.ofEpochMilli(context.window().getEnd()));

            // Record metrics for this window
            contentMetrics.recordWindow(
                    (int) metrics.getViewCount(),
                    metrics.getUniqueViewerIds() != null ? metrics.getUniqueViewerIds().size() : 0,
                    metrics.getTotalWatchTimeMs(),
                    (int) metrics.getCompletedViews()
            );

            out.collect(metrics);
        }
    }
}
