package com.engine.walpulse.domain.port.out;

import com.engine.walpulse.domain.model.SinkRecord;
import com.engine.walpulse.domain.model.WalChangeRecord;

import java.util.Optional;

/**
 * Outbound port for filtering and transforming WAL change records into sink-ready records.
 */
public interface TransformEnginePort {

    /**
     * Filters, masks, and converts a raw WAL change record.
     *
     * @param changeRecord incoming database change
     * @return Optional containing SinkRecord, or empty if filtered out
     */
    Optional<SinkRecord> transform(WalChangeRecord changeRecord);
}
