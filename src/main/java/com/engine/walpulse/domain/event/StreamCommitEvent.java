package com.engine.walpulse.domain.event;

import com.engine.walpulse.domain.model.LsnPosition;

import java.time.Instant;
import java.util.Objects;

/**
 * Event representing the commit boundary of a database transaction in WAL.
 */
public record StreamCommitEvent(
        long transactionId,
        LsnPosition commitLsn,
        LsnPosition endLsn,
        Instant commitTimestamp
) {
    public StreamCommitEvent {
        Objects.requireNonNull(commitLsn, "commitLsn must not be null");
        Objects.requireNonNull(endLsn, "endLsn must not be null");
        Objects.requireNonNull(commitTimestamp, "commitTimestamp must not be null");
    }
}
