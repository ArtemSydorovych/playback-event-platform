package com.netflix.playback.generator;

import com.netflix.playback.model.PlaybackEvent;
import com.netflix.playback.serialization.JsonSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple Kafka producer for PlaybackEvents.
 */
public class SimpleEventProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SimpleEventProducer.class);

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final AtomicLong sentCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    public SimpleEventProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        this.producer = new KafkaProducer<>(createConfig(bootstrapServers));
        log.info("Producer created for topic '{}' at {}", topic, bootstrapServers);
    }

    private Properties createConfig(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "event-generator");
        return props;
    }

    public Future<RecordMetadata> send(PlaybackEvent event) {
        String key = event.getUserId();
        String value = JsonSerializer.serializeCompact(event);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        return producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                errorCount.incrementAndGet();
                log.error("Send failed: {}", exception.getMessage());
            } else {
                sentCount.incrementAndGet();
                log.debug("Sent to partition {} offset {}", metadata.partition(), metadata.offset());
            }
        });
    }

    public void flush() {
        producer.flush();
    }

    public long getSentCount() {
        return sentCount.get();
    }

    public long getErrorCount() {
        return errorCount.get();
    }

    @Override
    public void close() {
        log.info("Closing producer. Sent: {}, Errors: {}", sentCount.get(), errorCount.get());
        producer.close();
    }
}
