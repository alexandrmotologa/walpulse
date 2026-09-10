package com.engine.walpulse.domain.port.out;

import com.engine.walpulse.domain.event.ReplicationEvent;
import com.engine.walpulse.domain.model.LsnPosition;

import java.util.Optional;

/**
 * Outbound port interfacing with PostgreSQL logical replication stream.
 */
public interface LogicalReplicationPort extends AutoCloseable {

    /**
     * Initializes replication slot and connects to the replication stream.
     */
    void connect();

    /**
     * Reads next available replication event from the replication stream (non-blocking).
     *
     * @return Optional containing ReplicationEvent if data is available, empty if no packet currently waiting
     */
    Optional<ReplicationEvent> readNextEvent();

    /**
     * Confirms the flushed LSN back to PostgreSQL so WAL can be safely reclaimed.
     *
     * @param lsn the confirmed LSN position
     */
    void updateFlushedLsn(LsnPosition lsn);

    /**
     * Returns the latest LSN received from the server.
     */
    LsnPosition getLastReceivedLsn();

    /**
     * Returns the latest LSN confirmed as flushed to sink.
     */
    LsnPosition getLastFlushedLsn();

    /**
     * Checks if the replication stream is currently connected and active.
     */
    boolean isConnected();

    /**
     * Closes the stream and connection.
     */
    @Override
    void close();
}
