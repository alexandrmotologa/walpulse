package com.engine.walpulse.domain.port.in;

/**
 * Inbound port for stopping the replication streaming pipeline gracefully.
 */
public interface StopReplicationUseCase {
    void stop();
}
