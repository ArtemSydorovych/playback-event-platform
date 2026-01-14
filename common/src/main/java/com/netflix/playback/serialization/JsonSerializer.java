package com.netflix.playback.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.netflix.playback.model.PlaybackEvent;

/**
 * JSON serialization utilities for PlaybackEvent.
 */
public class JsonSerializer {

    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // Support Java 8 date/time types
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Be lenient on deserialization
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        // Pretty print for readability (can be disabled in production)
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        return mapper;
    }

    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }

    /**
     * Serialize a PlaybackEvent to JSON string.
     */
    public static String serialize(PlaybackEvent event) {
        try {
            return OBJECT_MAPPER.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize PlaybackEvent", e);
        }
    }

    /**
     * Serialize a PlaybackEvent to compact JSON (no pretty print).
     */
    public static String serializeCompact(PlaybackEvent event) {
        try {
            return OBJECT_MAPPER.writer()
                .without(SerializationFeature.INDENT_OUTPUT)
                .writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize PlaybackEvent", e);
        }
    }

    /**
     * Deserialize a JSON string to PlaybackEvent.
     */
    public static PlaybackEvent deserialize(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, PlaybackEvent.class);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to deserialize PlaybackEvent", e);
        }
    }

    /**
     * Deserialize a JSON byte array to PlaybackEvent.
     */
    public static PlaybackEvent deserialize(byte[] json) {
        try {
            return OBJECT_MAPPER.readValue(json, PlaybackEvent.class);
        } catch (Exception e) {
            throw new SerializationException("Failed to deserialize PlaybackEvent", e);
        }
    }

    public static class SerializationException extends RuntimeException {
        public SerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
