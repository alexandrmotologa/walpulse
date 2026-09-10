package com.engine.walpulse.infrastructure.adapter.out.sink;

import com.engine.walpulse.domain.model.SinkRecord;
import com.engine.walpulse.domain.port.out.EventSinkPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Standard logging sink that writes records to logger/console.
 * Zero-broker sink used for local development and debug pipelines.
 */
public class LoggingSinkAdapter implements EventSinkPort {

    private static final Logger log = LoggerFactory.getLogger(LoggingSinkAdapter.class);

    @Override
    public CompletableFuture<Void> send(SinkRecord record) {
        log.info("[WALPULSE-LOG-SINK] Destination='{}' PartitionKey='{}' Op='{}' LSN='{}'\nPayload={}",
                record.destination(), record.partitionKey(), record.operation(), record.lsn(), record.payloadJson());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public String getSinkName() {
        return "logging";
    }
}
