package com.engine.walpulse.infrastructure.adapter.in.rest;

import com.engine.walpulse.application.service.ReplicationCoordinator;
import com.engine.walpulse.domain.model.ReplicationStatus;
import com.engine.walpulse.domain.model.WalChangeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Control plane REST endpoints for managing the replication stream and consuming live SSE events.
 */
@RestController
@RequestMapping("/api/v1")
public class ControlPlaneController {

    private static final Logger log = LoggerFactory.getLogger(ControlPlaneController.class);

    private final ReplicationCoordinator coordinator;

    public ControlPlaneController(ReplicationCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @GetMapping("/status")
    public ResponseEntity<ReplicationStatus> getStatus() {
        return ResponseEntity.ok(coordinator.getStatus());
    }

    @PostMapping("/replication/start")
    public ResponseEntity<Map<String, String>> startReplication() {
        coordinator.start();
        return ResponseEntity.ok(Map.of("status", "started", "message", "Replication coordinator initiated"));
    }

    @PostMapping("/replication/stop")
    public ResponseEntity<Map<String, String>> stopReplication() {
        coordinator.stop();
        return ResponseEntity.ok(Map.of("status", "stopped", "message", "Replication coordinator stopped"));
    }

    @GetMapping(value = "/stream/live", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamLiveEvents() {
        SseEmitter emitter = new SseEmitter(0L); // Infinite timeout for stream

        Consumer<WalChangeRecord> listener = record -> {
            try {
                Map<String, Object> eventData = new LinkedHashMap<>();
                eventData.put("id", record.transactionId() + ":" + record.lsn().asString());
                eventData.put("op", record.operation().name());
                eventData.put("schema", record.schemaName());
                eventData.put("table", record.tableName());
                eventData.put("lsn", record.lsn().asString());
                eventData.put("timestamp", record.commitTimestamp() != null ? record.commitTimestamp().toString() : null);
                eventData.put("before", record.beforeMap());
                eventData.put("after", record.afterMap());

                emitter.send(SseEmitter.event()
                        .name("wal_change")
                        .data(eventData, MediaType.APPLICATION_JSON));
            } catch (IOException e) {
                log.debug("SSE client disconnected: {}", e.getMessage());
                emitter.complete();
            }
        };

        coordinator.registerLiveListener(listener);

        emitter.onCompletion(() -> coordinator.unregisterLiveListener(listener));
        emitter.onTimeout(() -> coordinator.unregisterLiveListener(listener));
        emitter.onError(e -> coordinator.unregisterLiveListener(listener));

        try {
            // Send initial connection greeting
            emitter.send(SseEmitter.event()
                    .name("connected")
                    .data(Map.of("message", "Attached to live WalPulse stream", "status", coordinator.getStatus())));
        } catch (IOException ignored) {}

        return emitter;
    }
}
