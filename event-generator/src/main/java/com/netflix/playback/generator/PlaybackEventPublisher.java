package com.netflix.playback.generator;

import com.netflix.playback.avro.PlaybackEvent;
import com.netflix.playback.config.KafkaConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes playback events to Kafka using Avro serialization with Schema Registry.
 * <p>
 * This publisher is the primary interface for sending playback telemetry from
 * video players to the event streaming platform. Events are partitioned by userId
 * to ensure ordering guarantees for user-specific event sequences.
 */
public class PlaybackEventPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PlaybackEventPublisher.class);

    private final KafkaProducer<String, PlaybackEvent> producer;
    private final String topic;
    private final AtomicLong publishedCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);

    /**
     * Create a publisher with default configuration.
     */
    public PlaybackEventPublisher() {
        this(KafkaConfig.BOOTSTRAP_SERVERS, KafkaConfig.SCHEMA_REGISTRY_URL, KafkaConfig.TOPIC_PLAYBACK_EVENTS);
    }

    /**
     * Create a publisher with custom configuration.
     */
    public PlaybackEventPublisher(String bootstrapServers, String schemaRegistryUrl, String topic) {
        this.topic = topic;
        this.producer = new KafkaProducer<>(createConfig(bootstrapServers, schemaRegistryUrl));
        log.info("PlaybackEventPublisher initialized - topic: '{}', bootstrap: {}", topic, bootstrapServers);
    }

    private Properties createConfig(String bootstrapServers, String schemaRegistryUrl) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Schema Registry configuration
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);

        // Producer reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, KafkaConfig.DEFAULT_ACKS);
        props.put(ProducerConfig.RETRIES_CONFIG, KafkaConfig.DEFAULT_RETRIES);
        props.put(ProducerConfig.LINGER_MS_CONFIG, KafkaConfig.DEFAULT_LINGER_MS);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, KafkaConfig.DEFAULT_BATCH_SIZE);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "playback-event-publisher");

        return props;
    }

    /**
     * Publish a playback event asynchronously.
     * <p>
     * Events are partitioned by userId to maintain ordering for user-specific sequences.
     *
     * @param event The playback event to publish
     * @return Future representing the pending send operation
     */
    public Future<RecordMetadata> publish(PlaybackEvent event) {
        String partitionKey = event.getUserId();
        ProducerRecord<String, PlaybackEvent> record = new ProducerRecord<>(topic, partitionKey, event);

        return producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                failedCount.incrementAndGet();
                log.error("Failed to publish event {} for user {}: {}",
                        event.getEventId(), event.getUserId(), exception.getMessage());
            } else {
                publishedCount.incrementAndGet();
                log.debug("Published event {} to partition {} offset {}",
                        event.getEventId(), metadata.partition(), metadata.offset());
            }
        });
    }

    /**
     * Flush any buffered events to ensure delivery.
     */
    public void flush() {
        producer.flush();
    }

    /**
     * Get the total number of successfully published events.
     */
    public long getPublishedCount() {
        return publishedCount.get();
    }

    /**
     * Get the total number of failed publish attempts.
     */
    public long getFailedCount() {
        return failedCount.get();
    }

    @Override
    public void close() {
        log.info("Closing PlaybackEventPublisher - Published: {}, Failed: {}",
                publishedCount.get(), failedCount.get());
        producer.close();
    }
}
