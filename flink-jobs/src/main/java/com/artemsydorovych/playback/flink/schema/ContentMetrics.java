package com.artemsydorovych.playback.flink.schema;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Aggregated metrics for a content item within a time window.
 * Used for real-time content analytics (trending, popular content, QoS).
 */
public class ContentMetrics implements Serializable {

    private static final long serialVersionUID = 1L;

    private String contentId;
    private Instant windowStart;
    private Instant windowEnd;

    // View metrics
    private long viewCount;
    private Set<String> uniqueViewerIds;
    private long completedViews;

    // Watch time
    private long totalWatchTimeMs;

    // Quality metrics
    private long rebufferCount;
    private long totalRebufferDurationMs;
    private long errorCount;

    // Interaction metrics
    private long pauseCount;
    private long seekCount;
    private long qualityChangeCount;

    // Device breakdown
    private long mobileViews;
    private long desktopViews;
    private long tvViews;
    private long otherDeviceViews;

    public ContentMetrics() {
        this.uniqueViewerIds = new HashSet<>();
    }

    public ContentMetrics(String contentId, Instant windowStart, Instant windowEnd) {
        this();
        this.contentId = contentId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    // Merge two metrics (for combining partial aggregations)
    public ContentMetrics merge(ContentMetrics other) {
        if (other == null) return this;

        this.viewCount += other.viewCount;
        this.uniqueViewerIds.addAll(other.uniqueViewerIds);
        this.completedViews += other.completedViews;
        this.totalWatchTimeMs += other.totalWatchTimeMs;
        this.rebufferCount += other.rebufferCount;
        this.totalRebufferDurationMs += other.totalRebufferDurationMs;
        this.errorCount += other.errorCount;
        this.pauseCount += other.pauseCount;
        this.seekCount += other.seekCount;
        this.qualityChangeCount += other.qualityChangeCount;
        this.mobileViews += other.mobileViews;
        this.desktopViews += other.desktopViews;
        this.tvViews += other.tvViews;
        this.otherDeviceViews += other.otherDeviceViews;

        return this;
    }

    // Getters and setters
    public String getContentId() { return contentId; }
    public void setContentId(String contentId) { this.contentId = contentId; }

    public Instant getWindowStart() { return windowStart; }
    public void setWindowStart(Instant windowStart) { this.windowStart = windowStart; }

    public Instant getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Instant windowEnd) { this.windowEnd = windowEnd; }

    public long getViewCount() { return viewCount; }
    public void setViewCount(long viewCount) { this.viewCount = viewCount; }

    public Set<String> getUniqueViewerIds() { return uniqueViewerIds; }
    public void setUniqueViewerIds(Set<String> uniqueViewerIds) { this.uniqueViewerIds = uniqueViewerIds; }

    public long getUniqueViewers() { return uniqueViewerIds.size(); }

    public long getCompletedViews() { return completedViews; }
    public void setCompletedViews(long completedViews) { this.completedViews = completedViews; }

    public long getTotalWatchTimeMs() { return totalWatchTimeMs; }
    public void setTotalWatchTimeMs(long totalWatchTimeMs) { this.totalWatchTimeMs = totalWatchTimeMs; }

    public long getAvgWatchTimeMs() {
        return viewCount > 0 ? totalWatchTimeMs / viewCount : 0;
    }

    public long getRebufferCount() { return rebufferCount; }
    public void setRebufferCount(long rebufferCount) { this.rebufferCount = rebufferCount; }

    public long getTotalRebufferDurationMs() { return totalRebufferDurationMs; }
    public void setTotalRebufferDurationMs(long totalRebufferDurationMs) { this.totalRebufferDurationMs = totalRebufferDurationMs; }

    public long getErrorCount() { return errorCount; }
    public void setErrorCount(long errorCount) { this.errorCount = errorCount; }

    public long getPauseCount() { return pauseCount; }
    public void setPauseCount(long pauseCount) { this.pauseCount = pauseCount; }

    public long getSeekCount() { return seekCount; }
    public void setSeekCount(long seekCount) { this.seekCount = seekCount; }

    public long getQualityChangeCount() { return qualityChangeCount; }
    public void setQualityChangeCount(long qualityChangeCount) { this.qualityChangeCount = qualityChangeCount; }

    public long getMobileViews() { return mobileViews; }
    public void setMobileViews(long mobileViews) { this.mobileViews = mobileViews; }

    public long getDesktopViews() { return desktopViews; }
    public void setDesktopViews(long desktopViews) { this.desktopViews = desktopViews; }

    public long getTvViews() { return tvViews; }
    public void setTvViews(long tvViews) { this.tvViews = tvViews; }

    public long getOtherDeviceViews() { return otherDeviceViews; }
    public void setOtherDeviceViews(long otherDeviceViews) { this.otherDeviceViews = otherDeviceViews; }

    // Incrementers for aggregation
    public void incrementViewCount() { this.viewCount++; }
    public void addViewer(String userId) { this.uniqueViewerIds.add(userId); }
    public void incrementCompletedViews() { this.completedViews++; }
    public void addWatchTime(long ms) { this.totalWatchTimeMs += ms; }
    public void incrementRebufferCount() { this.rebufferCount++; }
    public void addRebufferDuration(long ms) { this.totalRebufferDurationMs += ms; }
    public void incrementErrorCount() { this.errorCount++; }
    public void incrementPauseCount() { this.pauseCount++; }
    public void incrementSeekCount() { this.seekCount++; }
    public void incrementQualityChangeCount() { this.qualityChangeCount++; }
    public void incrementMobileViews() { this.mobileViews++; }
    public void incrementDesktopViews() { this.desktopViews++; }
    public void incrementTvViews() { this.tvViews++; }
    public void incrementOtherDeviceViews() { this.otherDeviceViews++; }

    @Override
    public String toString() {
        return String.format(
            "ContentMetrics[contentId=%s, window=%s-%s, views=%d, uniqueViewers=%d, errors=%d]",
            contentId, windowStart, windowEnd, viewCount, getUniqueViewers(), errorCount
        );
    }
}
