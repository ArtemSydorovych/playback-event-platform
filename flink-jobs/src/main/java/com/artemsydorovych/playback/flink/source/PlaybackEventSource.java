package com.artemsydorovych.playback.flink.source;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.flink.config.JobParameters;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating Kafka source with Avro deserialization.
 */
public class PlaybackEventSource {

    private static final Logger log = LoggerFactory.getLogger(PlaybackEventSource.class);

    private PlaybackEventSource() {}

    /**
     * Creates a KafkaSource for PlaybackEvent with Schema Registry integration.
     */
    public static KafkaSource<PlaybackEvent> create(JobParameters params) {
        return createWithGroupId(params, params.getKafkaGroupId());
    }

    /**
     * Creates a KafkaSource with a custom consumer group ID.
     * Used by Production split jobs where each job has its own consumer group.
     */
    public static KafkaSource<PlaybackEvent> createWithGroupId(JobParameters params, String groupId) {
        log.info("Creating Kafka source: topic={}, bootstrap={}, registry={}, groupId={}",
            params.getKafkaTopic(),
            params.getKafkaBootstrapServers(),
            params.getSchemaRegistryUrl(),
            groupId);

        return KafkaSource.<PlaybackEvent>builder()
            .setBootstrapServers(params.getKafkaBootstrapServers())
            .setTopics(params.getKafkaTopic())
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new AvroDeserializationSchema(params.getSchemaRegistryUrl()))
            .build();
    }

    /**
     * Custom deserialization schema that integrates with Confluent Schema Registry.
     */
    private static class AvroDeserializationSchema
            implements KafkaRecordDeserializationSchema<PlaybackEvent> {

        private static final long serialVersionUID = 1L;

        private final String schemaRegistryUrl;
        private transient KafkaAvroDeserializer deserializer;

        public AvroDeserializationSchema(String schemaRegistryUrl) {
            this.schemaRegistryUrl = schemaRegistryUrl;
        }

        @Override
        public void open(DeserializationSchema.InitializationContext context) {
            Map<String, Object> config = new HashMap<>();
            config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

            deserializer = new KafkaAvroDeserializer();
            deserializer.configure(config, false);

            log.info("Avro deserializer initialized with Schema Registry: {}", schemaRegistryUrl);
        }

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<PlaybackEvent> out)
                throws IOException {
            try {
                Object deserialized = deserializer.deserialize(record.topic(), record.value());
                if (deserialized instanceof PlaybackEvent event) {
                    out.collect(event);
                } else {
                    log.warn("Unexpected type deserialized: {}", deserialized.getClass());
                }
            } catch (Exception e) {
                log.error("Failed to deserialize record from partition {} offset {}",
                    record.partition(), record.offset(), e);
                // Skip bad records rather than failing the job
            }
        }

        @Override
        public org.apache.flink.api.common.typeinfo.TypeInformation<PlaybackEvent> getProducedType() {
            return org.apache.flink.api.common.typeinfo.TypeInformation.of(PlaybackEvent.class);
        }
    }
}
