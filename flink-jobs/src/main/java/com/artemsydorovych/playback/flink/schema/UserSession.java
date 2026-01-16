package com.artemsydorovych.playback.flink.schema;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a user's playback session.
 * Session boundaries are detected via 30-minute inactivity gaps.
 */
public class UserSession implements Serializable {

    private static final long serialVersionUID = 1L;

    private String sessionId;
    private String userId;
    private String deviceId;
    private String deviceType;

    private Instant sessionStart;
    private Instant sessionEnd;
    private Instant lastEventTime;

    // Content watched in this session
    private Set<String> contentIds;

    // Event counts
    private int totalEvents;
    private int playStartCount;
    private int pauseCount;
    private int seekCount;
    private int errorCount;

    // Watch metrics
    private long totalWatchTimeMs;
    private long totalRebufferDurationMs;
    private int rebufferCount;

    // Completion tracking
    private double totalCompletionPercentage;
    private int completionSampleCount;

    public UserSession() {
        this.contentIds = new HashSet<>();
    }

    public UserSession(String userId, String sessionId, Instant sessionStart) {
        this();
        this.userId = userId;
        this.sessionId = sessionId;
        this.sessionStart = sessionStart;
        this.sessionEnd = sessionStart;
        this.lastEventTime = sessionStart;
    }

    // Computed properties
    public long getSessionDurationMs() {
        if (sessionStart == null || sessionEnd == null) return 0;
        return sessionEnd.toEpochMilli() - sessionStart.toEpochMilli();
    }

    public int getContentCount() {
        return contentIds.size();
    }

    public double getAvgCompletionPercentage() {
        return completionSampleCount > 0 ? totalCompletionPercentage / completionSampleCount : 0.0;
    }

    // Update methods for aggregation
    public void updateSessionEnd(Instant eventTime) {
        if (eventTime != null && (sessionEnd == null || eventTime.isAfter(sessionEnd))) {
            this.sessionEnd = eventTime;
        }
        this.lastEventTime = eventTime;
    }

    public void addContent(String contentId) {
        if (contentId != null) {
            this.contentIds.add(contentId);
        }
    }

    public void incrementTotalEvents() {
        this.totalEvents++;
    }

    public void incrementPlayStartCount() {
        this.playStartCount++;
    }

    public void incrementPauseCount() {
        this.pauseCount++;
    }

    public void incrementSeekCount() {
        this.seekCount++;
    }

    public void incrementErrorCount() {
        this.errorCount++;
    }

    public void addWatchTime(long ms) {
        this.totalWatchTimeMs += ms;
    }

    public void addRebuffer(long durationMs) {
        this.rebufferCount++;
        this.totalRebufferDurationMs += durationMs;
    }

    public void addCompletionSample(double percentage) {
        this.totalCompletionPercentage += percentage;
        this.completionSampleCount++;
    }

    // Getters and setters
    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }

    public String getDeviceType() { return deviceType; }
    public void setDeviceType(String deviceType) { this.deviceType = deviceType; }

    public Instant getSessionStart() { return sessionStart; }
    public void setSessionStart(Instant sessionStart) { this.sessionStart = sessionStart; }

    public Instant getSessionEnd() { return sessionEnd; }
    public void setSessionEnd(Instant sessionEnd) { this.sessionEnd = sessionEnd; }

    public Instant getLastEventTime() { return lastEventTime; }
    public void setLastEventTime(Instant lastEventTime) { this.lastEventTime = lastEventTime; }

    public Set<String> getContentIds() { return contentIds; }
    public void setContentIds(Set<String> contentIds) { this.contentIds = contentIds; }

    public int getTotalEvents() { return totalEvents; }
    public void setTotalEvents(int totalEvents) { this.totalEvents = totalEvents; }

    public int getPlayStartCount() { return playStartCount; }
    public void setPlayStartCount(int playStartCount) { this.playStartCount = playStartCount; }

    public int getPauseCount() { return pauseCount; }
    public void setPauseCount(int pauseCount) { this.pauseCount = pauseCount; }

    public int getSeekCount() { return seekCount; }
    public void setSeekCount(int seekCount) { this.seekCount = seekCount; }

    public int getErrorCount() { return errorCount; }
    public void setErrorCount(int errorCount) { this.errorCount = errorCount; }

    public long getTotalWatchTimeMs() { return totalWatchTimeMs; }
    public void setTotalWatchTimeMs(long totalWatchTimeMs) { this.totalWatchTimeMs = totalWatchTimeMs; }

    public long getTotalRebufferDurationMs() { return totalRebufferDurationMs; }
    public void setTotalRebufferDurationMs(long totalRebufferDurationMs) { this.totalRebufferDurationMs = totalRebufferDurationMs; }

    public int getRebufferCount() { return rebufferCount; }
    public void setRebufferCount(int rebufferCount) { this.rebufferCount = rebufferCount; }

    @Override
    public String toString() {
        return String.format(
            "UserSession[userId=%s, sessionId=%s, duration=%dms, contents=%d, events=%d]",
            userId, sessionId, getSessionDurationMs(), getContentCount(), totalEvents
        );
    }
}
