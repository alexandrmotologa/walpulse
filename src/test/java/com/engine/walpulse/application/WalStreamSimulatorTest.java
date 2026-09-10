package com.engine.walpulse.application;

import com.engine.walpulse.application.dto.WalPulseProperties;
import com.engine.walpulse.application.service.LsnTrackerService;
import com.engine.walpulse.application.service.ReplicationCoordinator;
import com.engine.walpulse.application.service.SchemaCacheService;
import com.engine.walpulse.application.service.WalStreamSimulator;
import com.engine.walpulse.domain.event.ChangeType;
import com.engine.walpulse.domain.model.SinkRecord;
import com.engine.walpulse.domain.model.WalChangeRecord;
import com.engine.walpulse.domain.port.out.DeadLetterQueuePort;
import com.engine.walpulse.domain.port.out.EventSinkPort;
import com.engine.walpulse.domain.port.out.LogicalReplicationPort;
import com.engine.walpulse.domain.port.out.TransformEnginePort;
import com.engine.walpulse.infrastructure.adapter.out.sink.MemoryDeadLetterQueueAdapter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class WalStreamSimulatorTest {

    private SchemaCacheService schemaCache;
    private WalStreamSimulator simulator;

    @BeforeEach
    void setUp() {
        LogicalReplicationPort replicationPort = Mockito.mock(LogicalReplicationPort.class);
        TransformEnginePort transformEngine = Mockito.mock(TransformEnginePort.class);
        EventSinkPort eventSink = Mockito.mock(EventSinkPort.class);
        schemaCache = new SchemaCacheService();
        LsnTrackerService lsnTracker = new LsnTrackerService();
        DeadLetterQueuePort dlq = new MemoryDeadLetterQueueAdapter();
        WalPulseProperties properties = new WalPulseProperties();

        when(eventSink.send(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(transformEngine.transform(any())).thenAnswer(inv -> {
            WalChangeRecord r = inv.getArgument(0);
            return Optional.of(new SinkRecord(
                    "sim:" + r.lsn().asString(),
                    "cdc." + r.fullTableName(),
                    "key",
                    "{}",
                    java.util.Map.of(),
                    r.lsn(),
                    r.commitTimestamp(),
                    r.operation()
            ));
        });

        ReplicationCoordinator coordinator = new ReplicationCoordinator(
                replicationPort,
                transformEngine,
                eventSink,
                schemaCache,
                lsnTracker,
                dlq,
                properties
        );

        simulator = new WalStreamSimulator(coordinator, schemaCache, new ObjectMapper());
    }

    @Test
    @DisplayName("Should simulate order creation, payment update, and deletion")
    void testSimulateLifecycle() {
        WalChangeRecord order = simulator.simulateOrderCreated("cust_123", 199.99);
        assertThat(order.operation()).isEqualTo(ChangeType.INSERT);
        assertThat(order.fullTableName()).isEqualTo("public.orders");
        assertThat(schemaCache.getRelationByName("public", "orders")).isPresent();

        long orderId = Long.parseLong((String) order.afterColumns().get(0).value());
        WalChangeRecord payment = simulator.simulatePaymentCompleted(orderId);
        assertThat(payment.operation()).isEqualTo(ChangeType.UPDATE);
        assertThat(payment.afterMap()).containsEntry("status", "PAID");

        WalChangeRecord delete = simulator.simulateCustomerDeleted(999);
        assertThat(delete.operation()).isEqualTo(ChangeType.DELETE);
        assertThat(delete.fullTableName()).isEqualTo("public.customers");
    }

    @Test
    @DisplayName("Should simulate schema evolution and record in schema history")
    void testSimulateSchemaEvolution() {
        simulator.simulateSchemaEvolution("public", "orders", "discount_code", "text");
        assertThat(schemaCache.getEvolutionLog()).isNotEmpty();
        assertThat(schemaCache.getEvolutionLog().get(0).description()).contains("discount_code");
    }
}
