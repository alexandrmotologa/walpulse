package com.engine.walpulse.application;

import com.engine.walpulse.application.dto.WalPulseProperties;
import com.engine.walpulse.application.service.LsnTrackerService;
import com.engine.walpulse.application.service.ReplicationCoordinator;
import com.engine.walpulse.application.service.SchemaCacheService;
import com.engine.walpulse.domain.event.ChangeType;
import com.engine.walpulse.domain.event.ReplicationEvent;
import com.engine.walpulse.domain.event.StreamCommitEvent;
import com.engine.walpulse.domain.model.ColumnValue;
import com.engine.walpulse.domain.model.LsnPosition;
import com.engine.walpulse.domain.model.ReplicationStatus;
import com.engine.walpulse.domain.model.SinkRecord;
import com.engine.walpulse.domain.model.WalChangeRecord;
import com.engine.walpulse.domain.port.out.EventSinkPort;
import com.engine.walpulse.domain.port.out.LogicalReplicationPort;
import com.engine.walpulse.domain.port.out.TransformEnginePort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReplicationCoordinatorTest {

    private LogicalReplicationPort replicationPort;
    private TransformEnginePort transformEngine;
    private EventSinkPort eventSink;
    private SchemaCacheService schemaCache;
    private LsnTrackerService lsnTracker;
    private WalPulseProperties properties;
    private ReplicationCoordinator coordinator;

    @BeforeEach
    void setUp() {
        replicationPort = Mockito.mock(LogicalReplicationPort.class);
        transformEngine = Mockito.mock(TransformEnginePort.class);
        eventSink = Mockito.mock(EventSinkPort.class);
        schemaCache = new SchemaCacheService();
        lsnTracker = new LsnTrackerService();
        properties = new WalPulseProperties();

        when(eventSink.send(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(replicationPort.isConnected()).thenReturn(true);

        coordinator = new ReplicationCoordinator(
                replicationPort,
                transformEngine,
                eventSink,
                schemaCache,
                lsnTracker,
                properties
        );
    }

    @Test
    @DisplayName("Should process DataChange event, dispatch to sink, and trigger live listeners")
    void testProcessDataChange() throws InterruptedException {
        LsnPosition lsn = LsnPosition.valueOf("0/16B4FE0");
        WalChangeRecord record = new WalChangeRecord(
                2001L,
                Instant.now(),
                lsn,
                ChangeType.INSERT,
                "public",
                "orders",
                List.of(),
                List.of(ColumnValue.of("id", 23, "int4", "101", true)),
                java.util.Map.of()
        );

        SinkRecord sinkRecord = new SinkRecord(
                "2001:0/16B4FE0",
                "cdc.public.orders",
                "101",
                "{\"id\":101}",
                java.util.Map.of(),
                lsn,
                Instant.now(),
                ChangeType.INSERT
        );

        when(transformEngine.transform(record)).thenReturn(Optional.of(sinkRecord));

        CountDownLatch eventLatch = new CountDownLatch(1);
        AtomicReference<WalChangeRecord> receivedInListener = new AtomicReference<>();

        coordinator.registerLiveListener(rec -> {
            receivedInListener.set(rec);
            eventLatch.countDown();
        });

        // Simulate port returning one event then stopping
        when(replicationPort.readNextEvent())
                .thenReturn(Optional.of(new ReplicationEvent.DataChange(record)))
                .thenReturn(Optional.empty());

        coordinator.start();

        boolean received = eventLatch.await(2, TimeUnit.SECONDS);
        coordinator.stop();

        assertThat(received).isTrue();
        assertThat(receivedInListener.get()).isEqualTo(record);
        verify(eventSink).send(sinkRecord);

        ReplicationStatus status = coordinator.getStatus();
        assertThat(status.lastReceivedLsn()).isEqualTo(lsn);
    }

    @Test
    @DisplayName("Should flush LSN feedback to replication port on transaction commit")
    void testCommitFeedback() throws InterruptedException {
        LsnPosition lsn = LsnPosition.valueOf("0/16B4FF0");
        lsnTracker.onLsnFlushed(lsn);

        StreamCommitEvent commitEvent = new StreamCommitEvent(
                2002L,
                lsn,
                lsn,
                Instant.now()
        );

        when(replicationPort.readNextEvent())
                .thenReturn(Optional.of(new ReplicationEvent.TransactionCommit(commitEvent)))
                .thenReturn(Optional.empty());

        coordinator.start();
        TimeUnit.MILLISECONDS.sleep(200);
        coordinator.stop();

        verify(replicationPort).updateFlushedLsn(lsn);
        assertThat(lsnTracker.getTotalTransactionsCommitted()).isEqualTo(1L);
    }
}
