package com.engine.walpulse.domain.port.out;

import com.engine.walpulse.domain.model.DlqRecord;
import com.engine.walpulse.domain.model.SinkRecord;

import java.util.List;
import java.util.Optional;

/**
 * Outbound port for managing Dead Letter Queue operations.
 */
public interface DeadLetterQueuePort {

    /**
     * Enqueues a failed sink delivery into the DLQ.
     */
    void enqueue(SinkRecord record, String errorReason);

    /**
     * Lists all failed records currently retained in the DLQ.
     */
    List<DlqRecord> listAll();

    /**
     * Finds a single DLQ record by identifier.
     */
    Optional<DlqRecord> findById(String id);

    /**
     * Removes a record from the DLQ after successful redrive or manual deletion.
     */
    boolean remove(String id);

    /**
     * Clears all records from the DLQ.
     */
    void clear();

    /**
     * Returns total number of failed records in the DLQ.
     */
    int size();
}
