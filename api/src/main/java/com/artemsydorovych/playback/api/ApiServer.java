package com.artemsydorovych.playback.api;

import com.artemsydorovych.playback.api.controller.ContentController;
import com.artemsydorovych.playback.api.controller.SessionController;
import com.artemsydorovych.playback.api.controller.UserController;
import com.artemsydorovych.playback.api.service.PlaybackService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST API server for playback data.
 * Stateless - reads from Cassandra.
 */
public class ApiServer {

    private static final Logger log = LoggerFactory.getLogger(ApiServer.class);
    private static final int DEFAULT_PORT = 8090;

    private final Javalin app;
    private final PlaybackService playbackService;

    public ApiServer() {
        this(DEFAULT_PORT);
    }

    public ApiServer(int port) {
        this.playbackService = new PlaybackService();

        // Configure Jackson for JSON serialization
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        this.app = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson(mapper, true));
            config.http.defaultContentType = "application/json";
        });

        registerRoutes();
        registerExceptionHandlers();

        app.events(event -> {
            event.serverStarted(() -> log.info("API server started on port {}", port));
            event.serverStopped(() -> {
                log.info("API server stopped");
                playbackService.close();
            });
        });
    }

    private void registerRoutes() {
        UserController userController = new UserController(playbackService);
        SessionController sessionController = new SessionController(playbackService);
        ContentController contentController = new ContentController(playbackService);

        // Health check
        app.get("/health", ctx -> ctx.json(new HealthResponse("ok")));

        // User endpoints
        app.get("/api/users/{userId}/continue-watching", userController::getContinueWatching);
        app.get("/api/users/{userId}/progress", userController::getAllProgress);
        app.get("/api/users/{userId}/progress/{contentId}", userController::getProgress);
        app.get("/api/users/{userId}/events", userController::getEvents);

        // Session endpoints
        app.get("/api/sessions/{sessionId}/events", sessionController::getSessionEvents);

        // Content endpoints
        app.get("/api/content/{contentId}/events", contentController::getContentEvents);
    }

    private void registerExceptionHandlers() {
        app.exception(IllegalArgumentException.class, (e, ctx) -> {
            ctx.status(400).json(new ErrorResponse("Bad Request", e.getMessage()));
        });

        app.exception(Exception.class, (e, ctx) -> {
            log.error("Unhandled exception", e);
            ctx.status(500).json(new ErrorResponse("Internal Server Error", e.getMessage()));
        });
    }

    public void start(int port) {
        app.start(port);
    }

    public void stop() {
        app.stop();
    }

    // Response records
    record HealthResponse(String status) {}
    record ErrorResponse(String error, String message) {}

    public static void main(String[] args) {
        int port = DEFAULT_PORT;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                log.warn("Invalid port argument, using default: {}", DEFAULT_PORT);
            }
        }

        ApiServer server = new ApiServer(port);
        server.start(port);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
    }
}
