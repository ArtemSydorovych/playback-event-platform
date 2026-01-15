package com.artemsydorovych.playback.cassandra;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.cassandra.writer.*;
import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Routes playback events to appropriate table writers.
 * Flink-compatible: Serializable with lazy initialization.
 *
 * Usage:
 * <pre>
 * EventRouter router = new EventRouter();
 * router.open(session);  // In Flink's open() or before processing
 * router.route(event);   // For each event
 * router.close();        // In Flink's close() or shutdown
 * </pre>
 */
public class EventRouter implements Serializable, AutoCloseable {

    private static final long serialVersionUID = 1L;

    private transient Logger log;
    private transient List<TableWriter> writers;
    private transient boolean initialized;

    /**
     * Initialize the router with a Cassandra session.
     * Must be called before routing events.
     */
    public void open(CqlSession session) {
        this.log = LoggerFactory.getLogger(EventRouter.class);
        this.writers = createWriters();

        for (TableWriter writer : writers) {
            writer.open(session);
        }

        this.initialized = true;
        log.info("EventRouter initialized with {} writers", writers.size());
    }

    /**
     * Route an event to all applicable writers.
     */
    public void route(PlaybackEvent event) {
        ensureInitialized();

        int written = 0;
        for (TableWriter writer : writers) {
            if (writer.accepts(event)) {
                try {
                    writer.write(event);
                    written++;
                } catch (Exception e) {
                    log.error("Failed to write to {}: {}", writer.getTableName(), e.getMessage());
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Routed event {} to {} tables", event.getEventId(), written);
        }
    }

    /**
     * Get the list of active writers.
     */
    public List<TableWriter> getWriters() {
        return writers != null ? List.copyOf(writers) : List.of();
    }

    @Override
    public void close() {
        if (writers != null) {
            for (TableWriter writer : writers) {
                try {
                    writer.close();
                } catch (Exception e) {
                    log.warn("Error closing writer {}: {}", writer.getTableName(), e.getMessage());
                }
            }
            writers.clear();
        }
        initialized = false;
    }

    /**
     * Create all table writers.
     * Override this method to customize which writers are active.
     */
    protected List<TableWriter> createWriters() {
        List<TableWriter> list = new ArrayList<>();
        list.add(new EventsByUserWriter());
        list.add(new EventsBySessionWriter());
        list.add(new EventsByContentWriter());
        list.add(new UserProgressWriter());
        list.add(new ContinueWatchingWriter());
        list.add(new QualityEventsWriter());
        return list;
    }

    private void ensureInitialized() {
        if (!initialized || writers == null) {
            throw new IllegalStateException("EventRouter not initialized. Call open() first.");
        }
    }
}
