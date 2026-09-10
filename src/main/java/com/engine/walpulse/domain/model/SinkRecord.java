package com.engine.walpulse.domain.model;

import com.engine.walpulse.domain.event.ChangeType;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates an event prepared and formatted for delivery to downstream sinks.
 */
public record SinkRecord(
        String id,
        String destination,
        String partitionKey,
        String payloadJson,
        Map<String, String> headers,
        LsnPosition lsn,
        Instant timestamp,
        ChangeType operation
) {
    public SinkRecord {
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(payloadJson, "payloadJson must not be null");
        Objects.requireNonNull(lsn, "lsn must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        Objects.requireNonNull(operation, "operation must not be null");
        headers = headers == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(headers));
    }
}
