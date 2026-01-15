package com.artemsydorovych.playback.avro;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Avro serialization/deserialization of PlaybackEvent.
 */
class PlaybackEventAvroTest {

    @Test
    @DisplayName("Should serialize and deserialize PlaybackEvent with SeekPayload")
    void testSerializeDeserializeWithSeekPayload() throws Exception {
        PlaybackEvent event = PlaybackEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(EventType.SEEK)
                .setTimestamp(Instant.now())
                .setUserId("user-1001")
                .setDeviceId("device-001")
                .setSessionId(UUID.randomUUID().toString())
                .setContentId("movie-001")
                .setPosition(30000L)
                .setDuration(7200000L)
                .setDeviceInfo(DeviceInfo.newBuilder()
                        .setDeviceType(DeviceType.IOS)
                        .setDeviceModel("iPhone 15 Pro")
                        .setAppVersion("2.0.1")
                        .build())
                .setPayload(SeekPayload.newBuilder()
                        .setFromPosition(10000L)
                        .setToPosition(30000L)
                        .build())
                .build();

        // Serialize
        byte[] bytes = serializeEvent(event);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        // Deserialize
        PlaybackEvent deserialized = deserializeEvent(bytes);

        // Verify core fields
        assertEquals(event.getEventId(), deserialized.getEventId());
        assertEquals(event.getEventType(), deserialized.getEventType());
        assertEquals(event.getUserId(), deserialized.getUserId());
        assertEquals(event.getContentId(), deserialized.getContentId());
        assertEquals(event.getPosition(), deserialized.getPosition());
        assertEquals(event.getDuration(), deserialized.getDuration());

        // Verify device info
        assertNotNull(deserialized.getDeviceInfo());
        assertEquals(DeviceType.IOS, deserialized.getDeviceInfo().getDeviceType());
        assertEquals("iPhone 15 Pro", deserialized.getDeviceInfo().getDeviceModel());

        // Verify payload
        assertNotNull(deserialized.getPayload());
        assertInstanceOf(SeekPayload.class, deserialized.getPayload());
        SeekPayload seekPayload = (SeekPayload) deserialized.getPayload();
        assertEquals(10000L, seekPayload.getFromPosition());
        assertEquals(30000L, seekPayload.getToPosition());
    }

    @Test
    @DisplayName("Should serialize and deserialize PlaybackEvent with QualityChangePayload")
    void testSerializeDeserializeWithQualityChangePayload() throws Exception {
        PlaybackEvent event = PlaybackEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(EventType.QUALITY_CHANGE)
                .setTimestamp(Instant.now())
                .setUserId("user-1002")
                .setDeviceId("device-002")
                .setSessionId(UUID.randomUUID().toString())
                .setContentId("series-s01e01")
                .setPayload(QualityChangePayload.newBuilder()
                        .setOldQuality(VideoQuality.HD_720P)
                        .setNewQuality(VideoQuality.HD_1080P)
                        .setOldBitrate(3000)
                        .setNewBitrate(5000)
                        .setReason("bandwidth_increase")
                        .setAutomatic(true)
                        .build())
                .build();

        byte[] bytes = serializeEvent(event);
        PlaybackEvent deserialized = deserializeEvent(bytes);

        assertInstanceOf(QualityChangePayload.class, deserialized.getPayload());
        QualityChangePayload payload = (QualityChangePayload) deserialized.getPayload();
        assertEquals(VideoQuality.HD_720P, payload.getOldQuality());
        assertEquals(VideoQuality.HD_1080P, payload.getNewQuality());
        assertEquals(3000, payload.getOldBitrate());
        assertEquals(5000, payload.getNewBitrate());
        assertTrue(payload.getAutomatic());
    }

    @Test
    @DisplayName("Should serialize and deserialize PlaybackEvent with ProgressPayload")
    void testSerializeDeserializeWithProgressPayload() throws Exception {
        PlaybackEvent event = PlaybackEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(EventType.PROGRESS)
                .setTimestamp(Instant.now())
                .setUserId("user-1003")
                .setDeviceId("device-003")
                .setSessionId(UUID.randomUUID().toString())
                .setContentId("doc-001")
                .setPayload(ProgressPayload.newBuilder()
                        .setBufferLength(25000L)
                        .setCurrentQuality(VideoQuality.UHD_4K)
                        .setCurrentBitrate(15000)
                        .setDroppedFrames(2)
                        .setPlaybackRate(1.0)
                        .setSubtitlesEnabled(true)
                        .setSubtitleLanguage("en")
                        .build())
                .build();

        byte[] bytes = serializeEvent(event);
        PlaybackEvent deserialized = deserializeEvent(bytes);

        assertInstanceOf(ProgressPayload.class, deserialized.getPayload());
        ProgressPayload payload = (ProgressPayload) deserialized.getPayload();
        assertEquals(25000L, payload.getBufferLength());
        assertEquals(VideoQuality.UHD_4K, payload.getCurrentQuality());
        assertEquals(15000, payload.getCurrentBitrate());
        assertTrue(payload.getSubtitlesEnabled());
    }

    @Test
    @DisplayName("Should serialize and deserialize PlaybackEvent with PlaybackEndPayload")
    void testSerializeDeserializeWithPlaybackEndPayload() throws Exception {
        PlaybackEvent event = PlaybackEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(EventType.PLAYBACK_END)
                .setTimestamp(Instant.now())
                .setUserId("user-1004")
                .setDeviceId("device-004")
                .setSessionId(UUID.randomUUID().toString())
                .setContentId("movie-002")
                .setPayload(PlaybackEndPayload.newBuilder()
                        .setReason(PlaybackEndReason.COMPLETED)
                        .setFinalPosition(7200000L)
                        .setTotalWatchTime(7150000L)
                        .setCompletionPercentage(99.3)
                        .build())
                .build();

        byte[] bytes = serializeEvent(event);
        PlaybackEvent deserialized = deserializeEvent(bytes);

        assertInstanceOf(PlaybackEndPayload.class, deserialized.getPayload());
        PlaybackEndPayload payload = (PlaybackEndPayload) deserialized.getPayload();
        assertEquals(PlaybackEndReason.COMPLETED, payload.getReason());
        assertEquals(7200000L, payload.getFinalPosition());
        assertEquals(99.3, payload.getCompletionPercentage(), 0.01);
    }

    @Test
    @DisplayName("Should serialize and deserialize PlaybackEvent with null payload (PLAY_START)")
    void testSerializeDeserializeWithNullPayload() throws Exception {
        PlaybackEvent event = PlaybackEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(EventType.PLAY_START)
                .setTimestamp(Instant.now())
                .setUserId("user-1005")
                .setDeviceId("device-005")
                .setSessionId(UUID.randomUUID().toString())
                .setContentId("series-s02e01")
                .setPayload(null)
                .build();

        byte[] bytes = serializeEvent(event);
        PlaybackEvent deserialized = deserializeEvent(bytes);

        assertEquals(EventType.PLAY_START, deserialized.getEventType());
        assertNull(deserialized.getPayload());
    }

    @Test
    @DisplayName("Should handle all event types without throwing")
    void testAllEventTypes() {
        for (EventType eventType : EventType.values()) {
            assertDoesNotThrow(() -> {
                PlaybackEvent event = PlaybackEvent.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setEventType(eventType)
                        .setTimestamp(Instant.now())
                        .setUserId("user-test")
                        .setDeviceId("device-test")
                        .setSessionId(UUID.randomUUID().toString())
                        .setContentId("content-test")
                        .build();

                byte[] bytes = serializeEvent(event);
                PlaybackEvent deserialized = deserializeEvent(bytes);
                assertEquals(eventType, deserialized.getEventType());
            }, "Failed for event type: " + eventType);
        }
    }

    @Test
    @DisplayName("Should preserve timestamp with millisecond precision")
    void testTimestampPrecision() throws Exception {
        Instant originalTimestamp = Instant.ofEpochMilli(1704067200123L); // Specific milliseconds

        PlaybackEvent event = PlaybackEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(EventType.PAUSE)
                .setTimestamp(originalTimestamp)
                .setUserId("user-timestamp")
                .setDeviceId("device-timestamp")
                .setSessionId(UUID.randomUUID().toString())
                .setContentId("content-timestamp")
                .build();

        byte[] bytes = serializeEvent(event);
        PlaybackEvent deserialized = deserializeEvent(bytes);

        assertEquals(originalTimestamp, deserialized.getTimestamp());
    }

    // Helper methods

    private byte[] serializeEvent(PlaybackEvent event) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<PlaybackEvent> writer = new SpecificDatumWriter<>(PlaybackEvent.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(event, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private PlaybackEvent deserializeEvent(byte[] bytes) throws Exception {
        DatumReader<PlaybackEvent> reader = new SpecificDatumReader<>(PlaybackEvent.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return reader.read(null, decoder);
    }
}
