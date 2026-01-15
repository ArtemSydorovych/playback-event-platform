-- Playback Event Platform - Cassandra Schema
-- Netflix-like query patterns for playback events

-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS playback
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE playback;

-- ============================================================================
-- Table 1: events_by_user
-- User activity timeline - "What has this user watched?"
-- Query: SELECT * FROM events_by_user WHERE user_id = ? LIMIT ?;
-- ============================================================================
CREATE TABLE IF NOT EXISTS events_by_user (
    user_id TEXT,
    timestamp TIMESTAMP,
    event_id TEXT,
    event_type TEXT,
    session_id TEXT,
    content_id TEXT,
    title TEXT,
    position BIGINT,
    duration BIGINT,
    device_type TEXT,
    PRIMARY KEY ((user_id), timestamp, event_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, event_id ASC)
   AND default_time_to_live = 7776000;  -- 90 days TTL

-- ============================================================================
-- Table 2: events_by_session
-- Session replay - "What happened in this playback session?"
-- Query: SELECT * FROM events_by_session WHERE session_id = ?;
-- ============================================================================
CREATE TABLE IF NOT EXISTS events_by_session (
    session_id TEXT,
    timestamp TIMESTAMP,
    event_id TEXT,
    event_type TEXT,
    user_id TEXT,
    content_id TEXT,
    title TEXT,
    position BIGINT,
    duration BIGINT,
    payload_json TEXT,
    PRIMARY KEY ((session_id), timestamp, event_id)
) WITH CLUSTERING ORDER BY (timestamp ASC, event_id ASC)
   AND default_time_to_live = 604800;  -- 7 days TTL

-- ============================================================================
-- Table 3: events_by_content
-- Content analytics - "Who is watching this content?"
-- Query: SELECT * FROM events_by_content WHERE content_id = ? LIMIT ?;
-- ============================================================================
CREATE TABLE IF NOT EXISTS events_by_content (
    content_id TEXT,
    timestamp TIMESTAMP,
    user_id TEXT,
    event_id TEXT,
    event_type TEXT,
    session_id TEXT,
    position BIGINT,
    device_type TEXT,
    country TEXT,
    PRIMARY KEY ((content_id), timestamp, user_id, event_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, user_id ASC, event_id ASC)
   AND default_time_to_live = 2592000;  -- 30 days TTL

-- ============================================================================
-- Table 4: user_content_progress
-- Resume playback - "Where did user leave off on this content?"
-- Query: SELECT * FROM user_content_progress WHERE user_id = ? AND content_id = ?;
-- Query: SELECT * FROM user_content_progress WHERE user_id = ?;
-- ============================================================================
CREATE TABLE IF NOT EXISTS user_content_progress (
    user_id TEXT,
    content_id TEXT,
    title TEXT,
    last_position BIGINT,
    duration BIGINT,
    completion_percentage DOUBLE,
    last_watched TIMESTAMP,
    total_watch_time BIGINT,
    session_id TEXT,
    device_type TEXT,
    country TEXT,
    PRIMARY KEY ((user_id), content_id)
);

-- ============================================================================
-- Table 5: continue_watching
-- Continue watching list - "User's incomplete content sorted by recency"
-- Query: SELECT * FROM continue_watching WHERE user_id = ? LIMIT ?;
-- ============================================================================
CREATE TABLE IF NOT EXISTS continue_watching (
    user_id TEXT,
    last_watched TIMESTAMP,
    content_id TEXT,
    title TEXT,
    last_position BIGINT,
    duration BIGINT,
    completion_percentage DOUBLE,
    PRIMARY KEY ((user_id), last_watched, content_id)
) WITH CLUSTERING ORDER BY (last_watched DESC, content_id ASC);

-- ============================================================================
-- Table 6: playback_quality_events
-- Quality analytics - "Track rebuffering and quality changes"
-- Query: SELECT * FROM playback_quality_events WHERE content_id = ? AND event_date = ?;
-- ============================================================================
CREATE TABLE IF NOT EXISTS playback_quality_events (
    content_id TEXT,
    event_date DATE,
    timestamp TIMESTAMP,
    event_id TEXT,
    event_type TEXT,
    user_id TEXT,
    session_id TEXT,
    device_type TEXT,
    country TEXT,
    old_quality TEXT,
    new_quality TEXT,
    bitrate INT,
    rebuffer_duration BIGINT,
    PRIMARY KEY ((content_id, event_date), timestamp, event_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, event_id ASC)
   AND default_time_to_live = 604800;  -- 7 days TTL

-- Print confirmation
SELECT 'Schema initialized successfully!' AS status;
