package com.engine.walpulse.domain.port.in;

import com.engine.walpulse.domain.model.ReplicationStatus;

/**
 * Inbound port for querying the live replication status, lag, and processed counts.
 */
public interface GetStreamStatusUseCase {
    ReplicationStatus getStatus();
}
