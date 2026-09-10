package com.engine.walpulse.infrastructure.config;

import com.engine.walpulse.application.dto.WalPulseProperties;
import com.engine.walpulse.application.service.LsnTrackerService;
import com.engine.walpulse.application.service.ReplicationCoordinator;
import com.engine.walpulse.application.service.SchemaCacheService;
import com.engine.walpulse.application.service.WalStreamSimulator;
import com.engine.walpulse.domain.port.out.DeadLetterQueuePort;
import com.engine.walpulse.domain.port.out.EventSinkPort;
import com.engine.walpulse.domain.port.out.LogicalReplicationPort;
import com.engine.walpulse.domain.port.out.TransformEnginePort;
import com.engine.walpulse.infrastructure.adapter.out.postgres.PGReplicationStreamAdapter;
import com.engine.walpulse.infrastructure.adapter.out.postgres.PgOutputDecoder;
import com.engine.walpulse.infrastructure.adapter.out.sink.CompositeSinkAdapter;
import com.engine.walpulse.infrastructure.adapter.out.sink.KafkaSinkAdapter;
import com.engine.walpulse.infrastructure.adapter.out.sink.LoggingSinkAdapter;
import com.engine.walpulse.infrastructure.adapter.out.sink.MemoryDeadLetterQueueAdapter;
import com.engine.walpulse.infrastructure.adapter.out.sink.WebhookSinkAdapter;
import com.engine.walpulse.infrastructure.adapter.out.transform.JsonTransformEngineAdapter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class WalPulseConfig {

    @Bean
    public SchemaCacheService schemaCacheService() {
        return new SchemaCacheService();
    }

    @Bean
    public LsnTrackerService lsnTrackerService() {
        return new LsnTrackerService();
    }

    @Bean
    public PgOutputDecoder pgOutputDecoder() {
        return new PgOutputDecoder();
    }

    @Bean
    @ConditionalOnMissingBean(DeadLetterQueuePort.class)
    public DeadLetterQueuePort deadLetterQueuePort() {
        return new MemoryDeadLetterQueueAdapter();
    }

    @Bean
    @ConditionalOnMissingBean(LogicalReplicationPort.class)
    public LogicalReplicationPort logicalReplicationPort(
            WalPulseProperties properties,
            PgOutputDecoder decoder,
            SchemaCacheService schemaCache
    ) {
        return new PGReplicationStreamAdapter(properties, decoder, schemaCache);
    }

    @Bean
    public TransformEnginePort transformEnginePort(WalPulseProperties properties, ObjectMapper objectMapper) {
        return new JsonTransformEngineAdapter(properties, objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean(EventSinkPort.class)
    public EventSinkPort eventSinkPort(WalPulseProperties properties) {
        String type = properties.getSink().getType().toLowerCase();
        return switch (type) {
            case "kafka" -> new KafkaSinkAdapter(properties.getSink().getKafka());
            case "webhook" -> new WebhookSinkAdapter(properties.getSink().getWebhook());
            case "composite" -> {
                List<EventSinkPort> sinks = new ArrayList<>();
                sinks.add(new LoggingSinkAdapter());
                sinks.add(new WebhookSinkAdapter(properties.getSink().getWebhook()));
                sinks.add(new KafkaSinkAdapter(properties.getSink().getKafka()));
                yield new CompositeSinkAdapter(sinks);
            }
            default -> new LoggingSinkAdapter();
        };
    }

    @Bean
    public ReplicationCoordinator replicationCoordinator(
            LogicalReplicationPort replicationPort,
            TransformEnginePort transformEngine,
            EventSinkPort eventSink,
            SchemaCacheService schemaCache,
            LsnTrackerService lsnTracker,
            DeadLetterQueuePort deadLetterQueuePort,
            WalPulseProperties properties
    ) {
        return new ReplicationCoordinator(
                replicationPort,
                transformEngine,
                eventSink,
                schemaCache,
                lsnTracker,
                deadLetterQueuePort,
                properties
        );
    }

    @Bean
    public WalStreamSimulator walStreamSimulator(
            ReplicationCoordinator coordinator,
            SchemaCacheService schemaCache,
            ObjectMapper objectMapper
    ) {
        return new WalStreamSimulator(coordinator, schemaCache, objectMapper);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady(ApplicationReadyEvent event) {
        ReplicationCoordinator coordinator = event.getApplicationContext().getBean(ReplicationCoordinator.class);
        // Start replication automatically on startup
        coordinator.start();
    }
}
