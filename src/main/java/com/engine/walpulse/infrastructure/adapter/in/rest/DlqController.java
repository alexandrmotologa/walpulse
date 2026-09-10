package com.engine.walpulse.infrastructure.adapter.in.rest;

import com.engine.walpulse.application.service.ReplicationCoordinator;
import com.engine.walpulse.domain.model.DlqRecord;
import com.engine.walpulse.domain.port.out.DeadLetterQueuePort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * REST controller for inspecting, clearing, and redriving events from the Dead Letter Queue.
 */
@RestController
@RequestMapping("/api/v1/dlq")
public class DlqController {

    private final DeadLetterQueuePort dlqPort;
    private final ReplicationCoordinator coordinator;

    public DlqController(DeadLetterQueuePort dlqPort, ReplicationCoordinator coordinator) {
        this.dlqPort = dlqPort;
        this.coordinator = coordinator;
    }

    @GetMapping
    public ResponseEntity<List<DlqRecord>> listAll() {
        return ResponseEntity.ok(dlqPort.listAll());
    }

    @PostMapping("/{id}/redrive")
    public ResponseEntity<Map<String, Object>> redrive(@PathVariable String id) {
        boolean success = coordinator.redrive(id);
        return ResponseEntity.ok(Map.of("id", id, "success", success));
    }

    @PostMapping("/redrive-all")
    public ResponseEntity<Map<String, Object>> redriveAll() {
        int count = coordinator.redriveAll();
        return ResponseEntity.ok(Map.of("redrivenCount", count));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> remove(@PathVariable String id) {
        boolean removed = dlqPort.remove(id);
        return ResponseEntity.ok(Map.of("id", id, "removed", removed));
    }

    @DeleteMapping
    public ResponseEntity<Map<String, String>> clear() {
        dlqPort.clear();
        return ResponseEntity.ok(Map.of("message", "DLQ cleared"));
    }
}
