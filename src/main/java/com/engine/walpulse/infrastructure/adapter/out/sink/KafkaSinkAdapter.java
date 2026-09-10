package com.engine.walpulse.infrastructure.adapter.out.sink;

import com.engine.walpulse.application.dto.WalPulseProperties;
import com.engine.walpulse.domain.model.SinkRecord;
import com.engine.walpulse.domain.port.out.EventSinkPort;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Idempotent Apache Kafka producer sink adapter.
 */
public class KafkaSinkAdapter implements EventSinkPort, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaSinkAdapter.class);

    private final KafkaProducer<String, String> producer;
    private final boolean initialized;

    public KafkaSinkAdapter(WalPulseProperties.KafkaProperties kafkaConfig) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, kafkaConfig.getAcks());
        props.put(ProducerConfig.RETRIES_CONFIG, kafkaConfig.getRetries());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        KafkaProducer<String, String> p = null;
        boolean init = false;
        try {
            p = new KafkaProducer<>(props);
            init = true;
            log.info("Connected KafkaSinkAdapter to bootstrap servers: {}", kafkaConfig.getBootstrapServers());
        } catch (Exception e) {
            log.warn("KafkaProducer could not be initialized immediately (will fallback to logging): {}", e.getMessage());
        }
        this.producer = p;
        this.initialized = init;
    }

    @Override
    public CompletableFuture<Void> send(SinkRecord record) {
        if (!initialized || producer == null) {
            log.info("[KAFKA-FALLBACK] Simulated publish to topic='{}' key='{}'", record.destination(), record.partitionKey());
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                record.destination(),
                record.partitionKey(),
                record.payloadJson()
        );

        // Add metadata headers
        if (record.headers() != null) {
            record.headers().forEach((k, v) ->
                    producerRecord.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
        }

        producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to publish record to Kafka topic: {}", record.destination(), exception);
                future.completeExceptionally(exception);
            } else {
                log.debug("Published record to topic {} partition {} offset {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
                future.complete(null);
            }
        });

        return future;
    }

    @Override
    public String getSinkName() {
        return "kafka";
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }
}
