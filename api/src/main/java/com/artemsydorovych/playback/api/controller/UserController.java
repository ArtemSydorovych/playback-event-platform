package com.artemsydorovych.playback.api.controller;

import com.artemsydorovych.playback.api.model.ContinueWatchingItem;
import com.artemsydorovych.playback.api.model.PlaybackEventDto;
import com.artemsydorovych.playback.api.model.UserProgress;
import com.artemsydorovych.playback.api.service.PlaybackService;
import com.artemsydorovych.playback.config.CassandraConfig;
import io.javalin.http.Context;

import java.util.List;

/**
 * REST controller for user-related endpoints.
 */
public class UserController {

    private final PlaybackService playbackService;

    public UserController(PlaybackService playbackService) {
        this.playbackService = playbackService;
    }

    /**
     * GET /api/users/{userId}/continue-watching
     * Get Streaming platform "Continue Watching" list.
     */
    public void getContinueWatching(Context ctx) {
        String userId = ctx.pathParam("userId");
        int limit = ctx.queryParamAsClass("limit", Integer.class)
            .getOrDefault(CassandraConfig.CONTINUE_WATCHING_LIMIT);

        List<ContinueWatchingItem> items = playbackService.getContinueWatching(userId, limit);
        ctx.json(new ContinueWatchingResponse(userId, items.size(), items));
    }

    /**
     * GET /api/users/{userId}/progress
     * Get all content progress for a user.
     */
    public void getAllProgress(Context ctx) {
        String userId = ctx.pathParam("userId");
        List<UserProgress> progress = playbackService.getAllProgress(userId);
        ctx.json(new UserProgressListResponse(userId, progress.size(), progress));
    }

    /**
     * GET /api/users/{userId}/progress/{contentId}
     * Get user's progress on specific content (for resume playback).
     */
    public void getProgress(Context ctx) {
        String userId = ctx.pathParam("userId");
        String contentId = ctx.pathParam("contentId");

        playbackService.getProgress(userId, contentId)
            .ifPresentOrElse(
                progress -> ctx.json(progress),
                () -> ctx.status(404).json(new NotFoundResponse("Progress not found for user " + userId + " on content " + contentId))
            );
    }

    /**
     * GET /api/users/{userId}/events
     * Get user's event history.
     */
    public void getEvents(Context ctx) {
        String userId = ctx.pathParam("userId");
        int limit = ctx.queryParamAsClass("limit", Integer.class)
            .getOrDefault(CassandraConfig.DEFAULT_PAGE_SIZE);

        List<PlaybackEventDto> events = playbackService.getEventsByUser(userId, limit);
        ctx.json(new EventListResponse(events.size(), events));
    }

    // Response records
    record ContinueWatchingResponse(String userId, int count, List<ContinueWatchingItem> items) {}
    record UserProgressListResponse(String userId, int count, List<UserProgress> progress) {}
    record EventListResponse(int count, List<PlaybackEventDto> events) {}
    record NotFoundResponse(String message) {}
}
