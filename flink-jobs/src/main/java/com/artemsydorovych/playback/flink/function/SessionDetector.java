package com.artemsydorovych.playback.flink.function;

import com.artemsydorovych.playback.avro.EventType;
import com.artemsydorovych.playback.avro.PlaybackEndPayload;
import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.avro.RebufferPayload;
import com.artemsydorovych.playback.flink.config.FlinkConfig;
import com.artemsydorovych.playback.flink.schema.UserSession;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Detects user sessions based on 30-minute inactivity gaps.
 * Uses event-time timers to emit sessions when gaps are detected.
 *
 * Key: userId
 * Input: PlaybackEvent
 * Output: UserSession (emitted when session ends)
 */
public class SessionDetector extends KeyedProcessFunction<String, PlaybackEvent, UserSession> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(SessionDetector.class);

    private final long sessionGapMs;

    private transient ValueState<UserSession> sessionState;
    private transient ValueState<Long> timerState;

    public SessionDetector() {
        this(FlinkConfig.SESSION_GAP);
    }

    public SessionDetector(Duration sessionGap) {
        this.sessionGapMs = sessionGap.toMillis();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // State for the current session being built
        ValueStateDescriptor<UserSession> sessionDescriptor = new ValueStateDescriptor<>(
            "user-session",
            TypeInformation.of(UserSession.class)
        );
        sessionState = getRuntimeContext().getState(sessionDescriptor);

        // State for tracking the current timer
        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
            "session-timer",
            Long.class
        );
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(
            PlaybackEvent event,
            Context ctx,
            Collector<UserSession> out) throws Exception {

        long eventTime = event.getTimestamp().toEpochMilli();
        String userId = ctx.getCurrentKey();

        UserSession currentSession = sessionState.value();

        if (currentSession == null) {
            // Start a new session
            currentSession = createNewSession(userId, event);
            log.debug("Started new session for user {}: {}", userId, currentSession.getSessionId());
        }

        // Update session with this event
        updateSession(currentSession, event);

        // Update the session state
        sessionState.update(currentSession);

        // Cancel existing timer if any
        Long existingTimer = timerState.value();
        if (existingTimer != null) {
            ctx.timerService().deleteEventTimeTimer(existingTimer);
        }

        // Register new timer for session gap
        long timerTime = eventTime + sessionGapMs;
        ctx.timerService().registerEventTimeTimer(timerTime);
        timerState.update(timerTime);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<UserSession> out) throws Exception {

        // Timer fired - session gap exceeded, emit the session
        UserSession session = sessionState.value();

        if (session != null) {
            log.info("Session ended for user {}: session={}, duration={}ms, events={}",
                ctx.getCurrentKey(),
                session.getSessionId(),
                session.getSessionDurationMs(),
                session.getTotalEvents());

            out.collect(session);

            // Clear state for next session
            sessionState.clear();
            timerState.clear();
        }
    }

    private UserSession createNewSession(String userId, PlaybackEvent event) {
        UserSession session = new UserSession(
            userId,
            UUID.randomUUID().toString(),
            event.getTimestamp()
        );

        // Set device info from first event
        session.setDeviceId(event.getDeviceId().toString());
        if (event.getDeviceInfo() != null && event.getDeviceInfo().getDeviceType() != null) {
            session.setDeviceType(event.getDeviceInfo().getDeviceType().name());
        }

        return session;
    }

    private void updateSession(UserSession session, PlaybackEvent event) {
        // Update session end time
        session.updateSessionEnd(event.getTimestamp());

        // Add content being watched
        session.addContent(event.getContentId().toString());

        // Increment event count
        session.incrementTotalEvents();

        // Process based on event type
        EventType eventType = event.getEventType();

        switch (eventType) {
            case PLAY_START -> session.incrementPlayStartCount();
            case PAUSE -> session.incrementPauseCount();
            case SEEK -> session.incrementSeekCount();
            case REBUFFER_END -> {
                if (event.getPayload() instanceof RebufferPayload rebuffer) {
                    Long duration = rebuffer.getRebufferDuration();
                    session.addRebuffer(duration != null ? duration : 0);
                } else {
                    session.addRebuffer(0);
                }
            }
            case PLAYBACK_END -> {
                if (event.getPayload() instanceof PlaybackEndPayload endPayload) {
                    long watchTime = endPayload.getTotalWatchTime();
                    session.addWatchTime(watchTime);
                    double completion = endPayload.getCompletionPercentage();
                    session.addCompletionSample(completion);
                }
            }
            default -> {
                // Other events don't need special handling
            }
        }
    }
}
