package com.artemsydorovych.playback.cassandra.writer;

import com.artemsydorovych.playback.avro.DeviceInfo;
import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Base class for table writers with common functionality.
 * Handles session management and common field extraction.
 */
public abstract class AbstractTableWriter implements TableWriter {

    private static final long serialVersionUID = 1L;

    protected transient Logger log;
    protected transient CqlSession session;
    protected transient PreparedStatement preparedStatement;

    @Override
    public void open(CqlSession session) {
        this.log = LoggerFactory.getLogger(getClass());
        this.session = session;
        this.preparedStatement = session.prepare(getCql());
        log.info("Initialized writer for table: {}", getTableName());
    }

    @Override
    public void close() {
        // Session is managed externally, don't close it here
        this.preparedStatement = null;
        this.session = null;
    }

    /**
     * Get the CQL INSERT statement for this table.
     */
    protected abstract String getCql();

    /**
     * Get event timestamp as Instant.
     */
    protected Instant toInstant(PlaybackEvent event) {
        return event.getTimestamp();
    }

    /**
     * Extract device type from event, with fallback.
     */
    protected String extractDeviceType(PlaybackEvent event) {
        DeviceInfo deviceInfo = event.getDeviceInfo();
        if (deviceInfo != null && deviceInfo.getDeviceType() != null) {
            return deviceInfo.getDeviceType().name();
        }
        return "UNKNOWN";
    }

    /**
     * Extract country from event device info.
     */
    protected String extractCountry(PlaybackEvent event) {
        DeviceInfo deviceInfo = event.getDeviceInfo();
        if (deviceInfo != null) {
            return deviceInfo.getCountry();
        }
        return null;
    }

    /**
     * Check if session is ready.
     */
    protected void ensureOpen() {
        if (session == null || preparedStatement == null) {
            throw new IllegalStateException("Writer not initialized. Call open() first.");
        }
    }
}
