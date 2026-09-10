package com.engine.walpulse.domain.port.out;

import com.engine.walpulse.domain.model.SinkRecord;

import java.util.concurrent.CompletableFuture;

/**
 * Outbound port for dispatching formatted records to downstream event brokers or HTTP sinks.
 */
public interface EventSinkPort {

    /**
     * Sends a record to the downstream sink.
     *
     * @param record the formatted sink record
     * @return CompletableFuture completed when the sink acknowledges persistence
     */
    CompletableFuture<Void> send(SinkRecord record);

    /**
     * Identifier for the sink implementation.
     */
    String getSinkName();
}
