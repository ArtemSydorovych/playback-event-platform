package com.artemsydorovych.playback.api.controller;

import com.artemsydorovych.playback.api.model.PlaybackEventDto;
import com.artemsydorovych.playback.api.service.PlaybackService;
import com.artemsydorovych.playback.config.CassandraConfig;
import io.javalin.http.Context;

import java.util.List;

/**
 * REST controller for content-related endpoints.
 */
public class ContentController {

    private final PlaybackService playbackService;

    public ContentController(PlaybackService playbackService) {
        this.playbackService = playbackService;
    }

    /**
     * GET /api/content/{contentId}/events
     * Get events for specific content (content analytics - who's watching).
     */
    public void getContentEvents(Context ctx) {
        String contentId = ctx.pathParam("contentId");
        int limit = ctx.queryParamAsClass("limit", Integer.class)
            .getOrDefault(CassandraConfig.DEFAULT_PAGE_SIZE);

        List<PlaybackEventDto> events = playbackService.getEventsByContent(contentId, limit);
        ctx.json(new ContentEventsResponse(contentId, events.size(), events));
    }

    // Response records
    record ContentEventsResponse(String contentId, int count, List<PlaybackEventDto> events) {}
}
