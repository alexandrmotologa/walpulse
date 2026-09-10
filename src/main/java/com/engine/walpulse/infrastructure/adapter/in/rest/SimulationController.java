package com.engine.walpulse.infrastructure.adapter.in.rest;

import com.engine.walpulse.application.service.ReplicationCoordinator;
import com.engine.walpulse.application.service.WalStreamSimulator;
import com.engine.walpulse.domain.event.ChangeType;
import com.engine.walpulse.domain.model.LsnPosition;
import com.engine.walpulse.domain.model.SinkRecord;
import com.engine.walpulse.domain.model.WalChangeRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * REST controller for triggering simulated PostgreSQL logical replication scenarios.
 */
@RestController
@RequestMapping("/api/v1/simulate")
public class SimulationController {

    private final WalStreamSimulator simulator;
    private final ReplicationCoordinator coordinator;

    public SimulationController(WalStreamSimulator simulator, ReplicationCoordinator coordinator) {
        this.simulator = simulator;
        this.coordinator = coordinator;
    }

    @PostMapping("/order")
    public ResponseEntity<WalChangeRecord> simulateOrder(@RequestBody(required = false) Map<String, Object> body) {
        String customerId = body != null && body.containsKey("customer_id") ?
                body.get("customer_id").toString() : "cust_" + ThreadLocalRandom.current().nextInt(100, 999);
        double amount = body != null && body.containsKey("amount") ?
                Double.parseDouble(body.get("amount").toString()) : ThreadLocalRandom.current().nextDouble(25.0, 350.0);

        WalChangeRecord record = simulator.simulateOrderCreated(customerId, amount);
        return ResponseEntity.ok(record);
    }

    @PostMapping("/payment")
    public ResponseEntity<WalChangeRecord> simulatePayment(@RequestParam(defaultValue = "10001") long orderId) {
        WalChangeRecord record = simulator.simulatePaymentCompleted(orderId);
        return ResponseEntity.ok(record);
    }

    @PostMapping("/delete")
    public ResponseEntity<WalChangeRecord> simulateDelete(@RequestParam(defaultValue = "42") long customerId) {
        WalChangeRecord record = simulator.simulateCustomerDeleted(customerId);
        return ResponseEntity.ok(record);
    }

    @PostMapping("/outbox")
    public ResponseEntity<WalChangeRecord> simulateOutbox(@RequestBody(required = false) Map<String, Object> body) {
        String aggType = body != null && body.containsKey("aggregate_type") ? body.get("aggregate_type").toString() : "Order";
        String aggId = body != null && body.containsKey("aggregate_id") ? body.get("aggregate_id").toString() : "ord_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String eventType = body != null && body.containsKey("event_type") ? body.get("event_type").toString() : "OrderShipped";
        String topic = body != null && body.containsKey("destination_topic") ? body.get("destination_topic").toString() : "logistics.shipments";

        Map<String, Object> payload = Map.of(
                "orderId", aggId,
                "carrier", "DHL Express",
                "trackingNumber", "TRACK" + ThreadLocalRandom.current().nextInt(100000, 999999)
        );

        WalChangeRecord record = simulator.simulateOutboxEvent(aggType, aggId, eventType, topic, payload);
        return ResponseEntity.ok(record);
    }

    @PostMapping("/schema")
    public ResponseEntity<Map<String, String>> simulateSchema(@RequestParam(defaultValue = "tracking_code") String column) {
        simulator.simulateSchemaEvolution("public", "orders", column, "text");
        return ResponseEntity.ok(Map.of("status", "ok", "message", "Schema evolution simulated. Added column: " + column));
    }

    @PostMapping("/burst")
    public ResponseEntity<Map<String, Object>> simulateBurst(
            @RequestParam(defaultValue = "50") int count,
            @RequestParam(defaultValue = "50") int delayMs
    ) {
        simulator.simulateBurst(count, delayMs);
        return ResponseEntity.ok(Map.of(
                "status", "started",
                "count", count,
                "delayMs", delayMs,
                "message", "Burst simulation running on Virtual Thread"
        ));
    }

    @PostMapping("/dlq-sample")
    public ResponseEntity<Map<String, String>> simulateDlqSample() {
        String id = "err_" + System.currentTimeMillis();
        SinkRecord record = new SinkRecord(
                id,
                "https://api.external-partner.com/webhook/cdc",
                "cust_999",
                "{\"errorSimulation\":true,\"table\":\"public.orders\",\"id\":999}",
                Map.of("source", "walpulse"),
                LsnPosition.valueOf("0/16B4FF8"),
                Instant.now(),
                ChangeType.INSERT
        );
        coordinator.getDeadLetterQueue().enqueue(record, "HTTP 504 Gateway Timeout: Endpoint did not respond within 5000ms");
        return ResponseEntity.ok(Map.of("id", id, "status", "enqueued"));
    }
}
