package com.engine.walpulse.application.service;

import com.engine.walpulse.application.dto.WalPulseProperties;
import com.engine.walpulse.domain.event.ReplicationEvent;
import com.engine.walpulse.domain.event.ReplicationState;
import com.engine.walpulse.domain.model.LsnPosition;
import com.engine.walpulse.domain.model.ReplicationStatus;
import com.engine.walpulse.domain.model.SinkRecord;
import com.engine.walpulse.domain.model.WalChangeRecord;
import com.engine.walpulse.domain.port.in.GetStreamStatusUseCase;
import com.engine.walpulse.domain.port.in.StartReplicationUseCase;
import com.engine.walpulse.domain.port.in.StopReplicationUseCase;
import com.engine.walpulse.domain.port.out.EventSinkPort;
import com.engine.walpulse.domain.port.out.LogicalReplicationPort;
import com.engine.walpulse.domain.port.out.TransformEnginePort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Orchestrates the end-to-end Change Data Capture streaming pipeline.
 * Runs on a dedicated Java 21 Virtual Thread and relies strictly on domain ports.
 */
public class ReplicationCoordinator implements StartReplicationUseCase, StopReplicationUseCase, GetStreamStatusUseCase {

    private static final Logger log = LoggerFactory.getLogger(ReplicationCoordinator.class);

    private final LogicalReplicationPort replicationPort;
    private final TransformEnginePort transformEngine;
    private final EventSinkPort eventSink;
    private final SchemaCacheService schemaCache;
    private final LsnTrackerService lsnTracker;
    private final WalPulseProperties properties;

    private final List<Consumer<WalChangeRecord>> liveEventListeners = new CopyOnWriteArrayList<>();

    private volatile boolean running = false;
    private volatile ReplicationState state = ReplicationState.IDLE;
    private volatile Instant connectedSince = null;
    private volatile String lastError = null;
    private Thread workerThread = null;

    public ReplicationCoordinator(
            LogicalReplicationPort replicationPort,
            TransformEnginePort transformEngine,
            EventSinkPort eventSink,
            SchemaCacheService schemaCache,
            LsnTrackerService lsnTracker,
            WalPulseProperties properties
    ) {
        this.replicationPort = replicationPort;
        this.transformEngine = transformEngine;
        this.eventSink = eventSink;
        this.schemaCache = schemaCache;
        this.lsnTracker = lsnTracker;
        this.properties = properties;
    }

    @Override
    public synchronized void start() {
        if (running) {
            log.warn("ReplicationCoordinator is already running");
            return;
        }

        this.running = true;
        this.state = ReplicationState.STARTING;
        this.lastError = null;

        this.workerThread = Thread.ofVirtual()
                .name("walpulse-coordinator")
                .start(this::runStreamingLoop);

        log.info("Started WalPulse replication coordinator on Virtual Thread");
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }

        log.info("Stopping WalPulse replication coordinator...");
        this.running = false;
        this.state = ReplicationState.STOPPING;

        if (workerThread != null) {
            workerThread.interrupt();
            try {
                workerThread.join(3000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        replicationPort.close();
        this.state = ReplicationState.STOPPED;
        this.connectedSince = null;
        log.info("WalPulse replication coordinator stopped");
    }

    @Override
    public ReplicationStatus getStatus() {
        return new ReplicationStatus(
                state,
                properties.getPostgres().getSlotName(),
                properties.getPostgres().getPublicationName(),
                lsnTracker.getLastReceivedLsn(),
                lsnTracker.getLastFlushedLsn(),
                lsnTracker.getLagBytes(),
                lsnTracker.getTotalEventsProcessed(),
                lsnTracker.getTotalTransactionsCommitted(),
                connectedSince,
                lastError
        );
    }

    public void registerLiveListener(Consumer<WalChangeRecord> listener) {
        liveEventListeners.add(listener);
    }

    public void unregisterLiveListener(Consumer<WalChangeRecord> listener) {
        liveEventListeners.remove(listener);
    }

    private void runStreamingLoop() {
        while (running) {
            try {
                if (!replicationPort.isConnected()) {
                    this.state = ReplicationState.STARTING;
                    replicationPort.connect();
                    this.connectedSince = Instant.now();
                    this.state = ReplicationState.STREAMING;
                    this.lastError = null;
                    log.info("Connected to PostgreSQL logical stream successfully");
                }

                Optional<ReplicationEvent> eventOpt = replicationPort.readNextEvent();
                if (eventOpt.isPresent()) {
                    processEvent(eventOpt.get());
                } else {
                    // Small sleep to yield virtual thread when no WAL bytes are waiting
                    TimeUnit.MILLISECONDS.sleep(10);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Replication stream error: {}", e.getMessage());
                this.lastError = e.getMessage();
                this.state = ReplicationState.FAILED;
                replicationPort.close();

                if (running) {
                    try {
                        log.info("Attempting reconnection in 5 seconds...");
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        this.state = ReplicationState.STOPPED;
    }

    private void processEvent(ReplicationEvent event) {
        switch (event) {
            case ReplicationEvent.RelationSchema rel -> {
                log.debug("Received relation definition for table: {}", rel.metadata().fullTableName());
                schemaCache.registerRelation(rel.metadata());
            }

            case ReplicationEvent.DataChange data -> {
                WalChangeRecord record = data.record();
                lsnTracker.onLsnReceived(record.lsn());
                notifyLiveListeners(record);

                Optional<SinkRecord> sinkRecordOpt = transformEngine.transform(record);
                if (sinkRecordOpt.isPresent()) {
                    SinkRecord sinkRecord = sinkRecordOpt.get();
                    eventSink.send(sinkRecord)
                            .thenRun(() -> {
                                lsnTracker.onLsnFlushed(record.lsn());
                                lsnTracker.incrementEvents();
                            })
                            .exceptionally(ex -> {
                                log.error("Failed to deliver record {} to sink", record.lsn(), ex);
                                return null;
                            });
                } else {
                    // Filtered out, record LSN as flushed so Postgres can advance
                    lsnTracker.onLsnFlushed(record.lsn());
                }
            }

            case ReplicationEvent.TransactionCommit commit -> {
                lsnTracker.incrementCommits();
                LsnPosition flushed = lsnTracker.getLastFlushedLsn();
                if (flushed.compareTo(LsnPosition.ZERO) > 0) {
                    replicationPort.updateFlushedLsn(flushed);
                }
            }

            case ReplicationEvent.TransactionBegin begin -> {
                log.trace("Transaction began: xid={}", begin.xid());
            }

            case ReplicationEvent.TableTruncate trunc -> {
                log.info("Truncate received for relation OIDs: {}", trunc.relationOids());
            }

            case ReplicationEvent.Heartbeat hb -> {
                log.trace("Server heartbeat LSN: {}", hb.serverLsn());
            }

            case ReplicationEvent.Ignored ign -> {
                log.trace("Ignored message type: {} ({})", ign.typeByte(), ign.reason());
            }
        }
    }

    private void notifyLiveListeners(WalChangeRecord record) {
        for (Consumer<WalChangeRecord> listener : liveEventListeners) {
            try {
                listener.accept(record);
            } catch (Exception e) {
                log.trace("Listener notification error", e);
            }
        }
    }
}
