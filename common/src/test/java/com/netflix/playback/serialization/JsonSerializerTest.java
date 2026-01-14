package com.netflix.playback.serialization;

import com.netflix.playback.model.*;
import com.netflix.playback.model.payload.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class JsonSerializerTest {

    @Test
    void shouldSerializeAndDeserializeBasicEvent() {
        PlaybackEvent event = PlaybackEvent.builder()
            .eventType(EventType.PLAY_START)
            .userId("user-123")
            .profileId("profile-1")
            .deviceId("device-456")
            .sessionId("session-789")
            .contentId("movie-001")
            .position(0L)
            .contentDuration(7200000L) // 2 hours
            .build();

        String json = JsonSerializer.serialize(event);
        assertNotNull(json);
        assertTrue(json.contains("PLAY_START"));

        PlaybackEvent deserialized = JsonSerializer.deserialize(json);
        assertEquals(event.getEventType(), deserialized.getEventType());
        assertEquals(event.getUserId(), deserialized.getUserId());
        assertEquals(event.getContentId(), deserialized.getContentId());
    }

    @Test
    void shouldSerializeEventWithDeviceInfo() {
        DeviceInfo deviceInfo = DeviceInfo.builder()
            .deviceType(DeviceType.IOS)
            .deviceModel("iPhone 15 Pro")
            .osVersion("17.2")
            .appVersion("8.45.0")
            .country("US")
            .region("CA")
            .build();

        PlaybackEvent event = PlaybackEvent.builder()
            .eventType(EventType.PROGRESS)
            .userId("user-123")
            .contentId("movie-001")
            .position(300000L) // 5 minutes
            .deviceInfo(deviceInfo)
            .build();

        String json = JsonSerializer.serialize(event);
        PlaybackEvent deserialized = JsonSerializer.deserialize(json);

        assertNotNull(deserialized.getDeviceInfo());
        assertEquals(DeviceType.IOS, deserialized.getDeviceInfo().getDeviceType());
        assertEquals("iPhone 15 Pro", deserialized.getDeviceInfo().getDeviceModel());
    }

    @Test
    void shouldSerializeSeekEvent() {
        SeekPayload seekPayload = SeekPayload.builder()
            .fromPosition(60000L)
            .toPosition(180000L)
            .build();

        PlaybackEvent event = PlaybackEvent.builder()
            .eventType(EventType.SEEK)
            .userId("user-123")
            .contentId("movie-001")
            .position(180000L)
            .payload(EventPayload.ofSeek(seekPayload))
            .build();

        String json = JsonSerializer.serialize(event);
        PlaybackEvent deserialized = JsonSerializer.deserialize(json);

        assertNotNull(deserialized.getPayload());
        assertNotNull(deserialized.getPayload().getSeek());
        assertEquals(60000L, deserialized.getPayload().getSeek().getFromPosition());
        assertEquals(180000L, deserialized.getPayload().getSeek().getToPosition());
        assertTrue(deserialized.getPayload().getSeek().isForwardSeek());
    }

    @Test
    void shouldSerializeQualityChangeEvent() {
        QualityChangePayload payload = QualityChangePayload.builder()
            .oldQuality(VideoQuality.HD_1080P)
            .newQuality(VideoQuality.HD_720P)
            .oldBitrate(5000)
            .newBitrate(3000)
            .reason("bandwidth_decrease")
            .automatic(true)
            .build();

        PlaybackEvent event = PlaybackEvent.builder()
            .eventType(EventType.QUALITY_CHANGE)
            .userId("user-123")
            .contentId("movie-001")
            .payload(EventPayload.ofQualityChange(payload))
            .build();

        String json = JsonSerializer.serialize(event);
        PlaybackEvent deserialized = JsonSerializer.deserialize(json);

        assertNotNull(deserialized.getPayload().getQualityChange());
        assertEquals(VideoQuality.HD_1080P, deserialized.getPayload().getQualityChange().getOldQuality());
        assertEquals(VideoQuality.HD_720P, deserialized.getPayload().getQualityChange().getNewQuality());
    }

    @Test
    void shouldSerializePlaybackEndEvent() {
        PlaybackEndPayload payload = PlaybackEndPayload.builder()
            .reason(PlaybackEndReason.COMPLETED)
            .completionPercentage(98.5)
            .watchDuration(7100000L)
            .build();

        PlaybackEvent event = PlaybackEvent.builder()
            .eventType(EventType.PLAYBACK_END)
            .userId("user-123")
            .contentId("movie-001")
            .position(7100000L)
            .contentDuration(7200000L)
            .payload(EventPayload.ofPlaybackEnd(payload))
            .build();

        String json = JsonSerializer.serialize(event);
        PlaybackEvent deserialized = JsonSerializer.deserialize(json);

        assertNotNull(deserialized.getPayload().getPlaybackEnd());
        assertEquals(PlaybackEndReason.COMPLETED, deserialized.getPayload().getPlaybackEnd().getReason());
        assertTrue(deserialized.isContentCompleted());
    }

    @Test
    void shouldCalculateCompletionPercentage() {
        PlaybackEvent event = PlaybackEvent.builder()
            .eventType(EventType.PROGRESS)
            .position(3600000L) // 1 hour
            .contentDuration(7200000L) // 2 hours
            .build();

        assertEquals(50.0, event.getCompletionPercentage(), 0.01);
        assertFalse(event.isContentCompleted());

        event.setPosition(6600000L); // 1:50
        assertTrue(event.isContentCompleted());
    }

    @Test
    void shouldFormatPosition() {
        PlaybackEvent event = PlaybackEvent.builder()
            .position(3723000L) // 1 hour, 2 minutes, 3 seconds
            .build();

        assertEquals("01:02:03", event.getFormattedPosition());
    }

    @Test
    void shouldHandleTimestampSerialization() {
        Instant now = Instant.now();
        PlaybackEvent event = PlaybackEvent.builder()
            .eventType(EventType.PLAY_START)
            .timestamp(now)
            .userId("user-123")
            .contentId("movie-001")
            .build();

        String json = JsonSerializer.serialize(event);
        PlaybackEvent deserialized = JsonSerializer.deserialize(json);

        assertEquals(now, deserialized.getTimestamp());
    }
}
