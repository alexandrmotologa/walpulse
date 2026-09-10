package com.engine.walpulse.domain.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Encapsulates a failed sink delivery stored in the Dead Letter Queue for inspection and redrive.
 */
public record DlqRecord(
        String id,
        SinkRecord sinkRecord,
        String errorReason,
        Instant failedAt,
        int retryCount
) {
    public DlqRecord {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(sinkRecord, "sinkRecord must not be null");
        Objects.requireNonNull(errorReason, "errorReason must not be null");
        Objects.requireNonNull(failedAt, "failedAt must not be null");
    }

    public DlqRecord withIncrementedRetry(String newError) {
        return new DlqRecord(id, sinkRecord, newError, Instant.now(), retryCount + 1);
    }
}
