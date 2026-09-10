package com.engine.walpulse.domain.model;

import com.engine.walpulse.domain.event.ReplicationState;

import java.time.Instant;

/**
 * Snapshot of the current replication engine status and metrics.
 */
public record ReplicationStatus(
        ReplicationState state,
        String slotName,
        String publicationName,
        LsnPosition lastReceivedLsn,
        LsnPosition lastFlushedLsn,
        long lagBytes,
        long totalEventsProcessed,
        long totalTransactionsCommitted,
        Instant connectedSince,
        String lastError
) {
    public static ReplicationStatus idle(String slotName, String publicationName) {
        return new ReplicationStatus(
                ReplicationState.IDLE,
                slotName,
                publicationName,
                LsnPosition.ZERO,
                LsnPosition.ZERO,
                0L,
                0L,
                0L,
                null,
                null
        );
    }
}
