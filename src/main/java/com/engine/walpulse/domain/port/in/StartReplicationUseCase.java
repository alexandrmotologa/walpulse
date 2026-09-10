package com.engine.walpulse.domain.port.in;

/**
 * Inbound port for starting the logical replication streaming pipeline.
 */
public interface StartReplicationUseCase {
    void start();
}
