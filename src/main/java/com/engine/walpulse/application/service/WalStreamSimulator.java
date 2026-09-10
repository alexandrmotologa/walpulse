package com.engine.walpulse.application.service;

import com.engine.walpulse.domain.event.ChangeType;
import com.engine.walpulse.domain.event.ReplicationEvent;
import com.engine.walpulse.domain.event.StreamCommitEvent;
import com.engine.walpulse.domain.model.ColumnValue;
import com.engine.walpulse.domain.model.LsnPosition;
import com.engine.walpulse.domain.model.TableMetadata;
import com.engine.walpulse.domain.model.WalChangeRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service providing realistic PostgreSQL CDC event simulation for demonstrations and local testing.
 */
public class WalStreamSimulator {

    private static final Logger log = LoggerFactory.getLogger(WalStreamSimulator.class);

    private final ReplicationCoordinator coordinator;
    private final SchemaCacheService schemaCache;
    private final ObjectMapper objectMapper;

    private final AtomicLong lsnCounter = new AtomicLong(0x1000000L);
    private final AtomicLong xidCounter = new AtomicLong(5000L);

    public WalStreamSimulator(
            ReplicationCoordinator coordinator,
            SchemaCacheService schemaCache,
            ObjectMapper objectMapper
    ) {
        this.coordinator = coordinator;
        this.schemaCache = schemaCache;
        this.objectMapper = objectMapper;
    }

    public WalChangeRecord simulateOrderCreated(String customerId, double amount) {
        ensureOrdersSchemaRegistered();

        long xid = xidCounter.incrementAndGet();
        LsnPosition lsn = nextLsn();
        Instant now = Instant.now();
        long orderId = ThreadLocalRandom.current().nextLong(10000, 99999);

        coordinator.processSimulatedEvent(new ReplicationEvent.TransactionBegin(xid, now, lsn));

        WalChangeRecord record = new WalChangeRecord(
                xid,
                now,
                lsn,
                ChangeType.INSERT,
                "public",
                "orders",
                List.of(),
                List.of(
                        ColumnValue.of("id", 23, "int4", String.valueOf(orderId), true),
                        ColumnValue.of("customer_id", 25, "text", customerId, false),
                        ColumnValue.of("total_amount", 701, "float8", String.format("%.2f", amount), false),
                        ColumnValue.of("status", 25, "text", "PENDING", false),
                        ColumnValue.of("created_at", 1184, "timestamptz", now.toString(), false)
                ),
                Map.of("simulation", "true")
        );

        coordinator.processSimulatedEvent(new ReplicationEvent.DataChange(record));
        coordinator.processSimulatedEvent(new ReplicationEvent.TransactionCommit(new StreamCommitEvent(xid, lsn, lsn, now)));

        return record;
    }

    public WalChangeRecord simulatePaymentCompleted(long orderId) {
        ensureOrdersSchemaRegistered();

        long xid = xidCounter.incrementAndGet();
        LsnPosition lsn = nextLsn();
        Instant now = Instant.now();

        coordinator.processSimulatedEvent(new ReplicationEvent.TransactionBegin(xid, now, lsn));

        WalChangeRecord record = new WalChangeRecord(
                xid,
                now,
                lsn,
                ChangeType.UPDATE,
                "public",
                "orders",
                List.of(
                        ColumnValue.of("id", 23, "int4", String.valueOf(orderId), true),
                        ColumnValue.of("status", 25, "text", "PENDING", false)
                ),
                List.of(
                        ColumnValue.of("id", 23, "int4", String.valueOf(orderId), true),
                        ColumnValue.of("status", 25, "text", "PAID", false),
                        ColumnValue.of("paid_at", 1184, "timestamptz", now.toString(), false)
                ),
                Map.of("simulation", "true")
        );

        coordinator.processSimulatedEvent(new ReplicationEvent.DataChange(record));
        coordinator.processSimulatedEvent(new ReplicationEvent.TransactionCommit(new StreamCommitEvent(xid, lsn, lsn, now)));

        return record;
    }

    public WalChangeRecord simulateCustomerDeleted(long customerId) {
        ensureCustomersSchemaRegistered();

        long xid = xidCounter.incrementAndGet();
        LsnPosition lsn = nextLsn();
        Instant now = Instant.now();

        coordinator.processSimulatedEvent(new ReplicationEvent.TransactionBegin(xid, now, lsn));

        WalChangeRecord record = new WalChangeRecord(
                xid,
                now,
                lsn,
                ChangeType.DELETE,
                "public",
                "customers",
                List.of(
                        ColumnValue.of("id", 23, "int4", String.valueOf(customerId), true),
                        ColumnValue.of("email", 25, "text", "user" + customerId + "@example.com", false)
                ),
                List.of(),
                Map.of("simulation", "true")
        );

        coordinator.processSimulatedEvent(new ReplicationEvent.DataChange(record));
        coordinator.processSimulatedEvent(new ReplicationEvent.TransactionCommit(new StreamCommitEvent(xid, lsn, lsn, now)));

        return record;
    }

    public WalChangeRecord simulateOutboxEvent(String aggregateType, String aggregateId, String eventType, String destinationTopic, Map<String, Object> payload) {
        ensureOutboxSchemaRegistered();

        long xid = xidCounter.incrementAndGet();
        LsnPosition lsn = nextLsn();
        Instant now = Instant.now();

        String payloadJson = "{}";
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (Exception ignored) {}

        coordinator.processSimulatedEvent(new ReplicationEvent.TransactionBegin(xid, now, lsn));

        WalChangeRecord record = new WalChangeRecord(
                xid,
                now,
                lsn,
                ChangeType.INSERT,
                "public",
                "outbox_messages",
                List.of(),
                List.of(
                        ColumnValue.of("id", 2950, "uuid", java.util.UUID.randomUUID().toString(), true),
                        ColumnValue.of("aggregate_type", 25, "text", aggregateType, false),
                        ColumnValue.of("aggregate_id", 25, "text", aggregateId, false),
                        ColumnValue.of("event_type", 25, "text", eventType, false),
                        ColumnValue.of("destination_topic", 25, "text", destinationTopic, false),
                        ColumnValue.of("payload", 3802, "jsonb", payloadJson, false)
                ),
                Map.of("simulation", "true")
        );

        coordinator.processSimulatedEvent(new ReplicationEvent.DataChange(record));
        coordinator.processSimulatedEvent(new ReplicationEvent.TransactionCommit(new StreamCommitEvent(xid, lsn, lsn, now)));

        return record;
    }

    public void simulateSchemaEvolution(String schema, String table, String newColName, String newColType) {
        String full = schema + "." + table;
        TableMetadata existing = schemaCache.getRelationByName(schema, table)
                .orElseGet(() -> {
                    ensureOrdersSchemaRegistered();
                    return schemaCache.getRelationByName(schema, table).get();
                });

        List<TableMetadata.ColumnDefinition> cols = new ArrayList<>(existing.columns());
        cols.add(new TableMetadata.ColumnDefinition(cols.size(), newColName, 25, -1, false));

        TableMetadata updated = new TableMetadata(existing.relationOid(), schema, table, existing.replicaIdentity(), cols);
        coordinator.processSimulatedEvent(new ReplicationEvent.RelationSchema(updated));
        log.info("Simulated schema migration for {}: added column {}", full, newColName);
    }

    public void simulateBurst(int count, int delayMs) {
        Thread.ofVirtual().name("walpulse-burst-sim").start(() -> {
            log.info("Starting burst simulation of {} events with {}ms delay", count, delayMs);
            String[] customers = {"cust_alpha", "cust_beta", "cust_gamma", "cust_delta", "cust_omega"};
            for (int i = 0; i < count; i++) {
                String customer = customers[ThreadLocalRandom.current().nextInt(customers.length)];
                double amount = ThreadLocalRandom.current().nextDouble(15.0, 450.0);
                WalChangeRecord order = simulateOrderCreated(customer, amount);

                if (ThreadLocalRandom.current().nextBoolean()) {
                    long orderId = Long.parseLong((String) order.afterColumns().get(0).value());
                    simulatePaymentCompleted(orderId);
                }

                if (delayMs > 0) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(delayMs);
                    } catch (InterruptedException ignored) {
                        break;
                    }
                }
            }
            log.info("Burst simulation of {} events finished", count);
        });
    }

    private LsnPosition nextLsn() {
        return LsnPosition.fromLong(lsnCounter.addAndGet(0x100L));
    }

    private void ensureOrdersSchemaRegistered() {
        if (schemaCache.getRelationByName("public", "orders").isEmpty()) {
            List<TableMetadata.ColumnDefinition> cols = List.of(
                    new TableMetadata.ColumnDefinition(0, "id", 23, -1, true),
                    new TableMetadata.ColumnDefinition(1, "customer_id", 25, -1, false),
                    new TableMetadata.ColumnDefinition(2, "total_amount", 701, -1, false),
                    new TableMetadata.ColumnDefinition(3, "status", 25, -1, false),
                    new TableMetadata.ColumnDefinition(4, "created_at", 1184, -1, false)
            );
            TableMetadata meta = new TableMetadata(16401, "public", "orders", 'd', cols);
            coordinator.processSimulatedEvent(new ReplicationEvent.RelationSchema(meta));
        }
    }

    private void ensureCustomersSchemaRegistered() {
        if (schemaCache.getRelationByName("public", "customers").isEmpty()) {
            List<TableMetadata.ColumnDefinition> cols = List.of(
                    new TableMetadata.ColumnDefinition(0, "id", 23, -1, true),
                    new TableMetadata.ColumnDefinition(1, "email", 25, -1, false),
                    new TableMetadata.ColumnDefinition(2, "created_at", 1184, -1, false)
            );
            TableMetadata meta = new TableMetadata(16402, "public", "customers", 'd', cols);
            coordinator.processSimulatedEvent(new ReplicationEvent.RelationSchema(meta));
        }
    }

    private void ensureOutboxSchemaRegistered() {
        if (schemaCache.getRelationByName("public", "outbox_messages").isEmpty()) {
            List<TableMetadata.ColumnDefinition> cols = List.of(
                    new TableMetadata.ColumnDefinition(0, "id", 2950, -1, true),
                    new TableMetadata.ColumnDefinition(1, "aggregate_type", 25, -1, false),
                    new TableMetadata.ColumnDefinition(2, "aggregate_id", 25, -1, false),
                    new TableMetadata.ColumnDefinition(3, "event_type", 25, -1, false),
                    new TableMetadata.ColumnDefinition(4, "destination_topic", 25, -1, false),
                    new TableMetadata.ColumnDefinition(5, "payload", 3802, -1, false)
            );
            TableMetadata meta = new TableMetadata(16403, "public", "outbox_messages", 'd', cols);
            coordinator.processSimulatedEvent(new ReplicationEvent.RelationSchema(meta));
        }
    }
}
