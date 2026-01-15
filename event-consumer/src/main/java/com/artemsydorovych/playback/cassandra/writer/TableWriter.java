package com.artemsydorovych.playback.cassandra.writer;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.datastax.oss.driver.api.core.CqlSession;

import java.io.Serializable;

/**
 * Interface for writing playback events to a specific Cassandra table.
 * Designed for Flink compatibility - implementations must be Serializable
 * and handle connection lifecycle via open()/close().
 */
public interface TableWriter extends Serializable, AutoCloseable {

    /**
     * Initialize the writer with a session.
     * Called once before processing begins (Flink's open() lifecycle).
     */
    void open(CqlSession session);

    /**
     * Check if this writer should handle the given event.
     */
    boolean accepts(PlaybackEvent event);

    /**
     * Write the event to the table.
     */
    void write(PlaybackEvent event);

    /**
     * Get the table name this writer handles.
     */
    String getTableName();
}
