package com.engine.walpulse.domain.port.in;

/**
 * Inbound port for re-dispatching failed records from the Dead Letter Queue back to target sinks.
 */
public interface RedriveDlqUseCase {

    /**
     * Attempts to re-deliver a specific record from DLQ to its destination sink.
     *
     * @param dlqId the DLQ record identifier
     * @return true if successfully delivered and removed from DLQ, false otherwise
     */
    boolean redrive(String dlqId);

    /**
     * Attempts to re-deliver all pending records in the DLQ.
     *
     * @return number of records successfully redriven
     */
    int redriveAll();
}
