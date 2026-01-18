package com.artemsydorovych.playback.flink.function;

import com.artemsydorovych.playback.avro.DeviceInfo;
import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.time.Instant;

/**
 * Maps PlaybackEvent (Avro) to Flink RowData for Iceberg sink.
 *
 * Schema:
 * - event_id (STRING)
 * - event_type (STRING)
 * - event_timestamp (TIMESTAMP)
 * - user_id (STRING)
 * - profile_id (STRING, nullable)
 * - device_id (STRING)
 * - session_id (STRING)
 * - content_id (STRING)
 * - content_type (STRING, nullable)
 * - title (STRING, nullable)
 * - position_ms (BIGINT, nullable)
 * - duration_ms (BIGINT, nullable)
 * - device_type (STRING, nullable)
 * - os_name (STRING, nullable)
 * - os_version (STRING, nullable)
 * - country (STRING, nullable)
 * - region (STRING, nullable)
 * - payload_json (STRING, nullable)
 */
public class PlaybackEventToRowDataMapper extends RichMapFunction<PlaybackEvent, RowData> {

    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public RowData map(PlaybackEvent event) throws Exception {
        GenericRowData row = new GenericRowData(18);

        // Required fields
        row.setField(0, StringData.fromString(event.getEventId().toString()));
        row.setField(1, StringData.fromString(event.getEventType().name()));
        row.setField(2, TimestampData.fromInstant(event.getTimestamp()));
        row.setField(3, StringData.fromString(event.getUserId().toString()));

        // Optional string fields
        row.setField(4, toStringData(event.getProfileId()));
        row.setField(5, StringData.fromString(event.getDeviceId().toString()));
        row.setField(6, StringData.fromString(event.getSessionId().toString()));
        row.setField(7, StringData.fromString(event.getContentId().toString()));
        row.setField(8, toStringData(event.getContentType()));
        row.setField(9, toStringData(event.getTitle()));

        // Position and duration
        row.setField(10, event.getPosition());
        row.setField(11, event.getDuration());

        // Device info (flattened)
        DeviceInfo deviceInfo = event.getDeviceInfo();
        if (deviceInfo != null) {
            row.setField(12, deviceInfo.getDeviceType() != null
                    ? StringData.fromString(deviceInfo.getDeviceType().name()) : null);
            row.setField(13, toStringData(deviceInfo.getOsName()));
            row.setField(14, toStringData(deviceInfo.getOsVersion()));
            row.setField(15, toStringData(deviceInfo.getCountry()));
            row.setField(16, toStringData(deviceInfo.getRegion()));
        } else {
            row.setField(12, null);
            row.setField(13, null);
            row.setField(14, null);
            row.setField(15, null);
            row.setField(16, null);
        }

        // Payload as JSON
        Object payload = event.getPayload();
        if (payload != null) {
            try {
                String payloadJson = objectMapper.writeValueAsString(payload);
                row.setField(17, StringData.fromString(payloadJson));
            } catch (Exception e) {
                row.setField(17, null);
            }
        } else {
            row.setField(17, null);
        }

        return row;
    }

    private StringData toStringData(CharSequence value) {
        return value != null ? StringData.fromString(value.toString()) : null;
    }
}
