package com.artemsydorovych.playback.api.controller;

import com.artemsydorovych.playback.api.model.PlaybackEventDto;
import com.artemsydorovych.playback.api.service.PlaybackService;
import io.javalin.http.Context;

import java.util.List;

/**
 * REST controller for session-related endpoints.
 */
public class SessionController {

    private final PlaybackService playbackService;

    public SessionController(PlaybackService playbackService) {
        this.playbackService = playbackService;
    }

    /**
     * GET /api/sessions/{sessionId}/events
     * Get all events for a playback session (session replay/debugging).
     */
    public void getSessionEvents(Context ctx) {
        String sessionId = ctx.pathParam("sessionId");
        List<PlaybackEventDto> events = playbackService.getEventsBySession(sessionId);

        if (events.isEmpty()) {
            ctx.status(404).json(new NotFoundResponse("Session not found: " + sessionId));
        } else {
            ctx.json(new SessionEventsResponse(sessionId, events.size(), events));
        }
    }

    // Response records
    record SessionEventsResponse(String sessionId, int count, List<PlaybackEventDto> events) {}
    record NotFoundResponse(String message) {}
}
