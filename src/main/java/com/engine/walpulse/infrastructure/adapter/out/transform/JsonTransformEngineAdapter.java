package com.engine.walpulse.infrastructure.adapter.out.transform;

import com.engine.walpulse.application.dto.WalPulseProperties;
import com.engine.walpulse.domain.event.ChangeType;
import com.engine.walpulse.domain.model.ColumnValue;
import com.engine.walpulse.domain.model.SinkRecord;
import com.engine.walpulse.domain.model.WalChangeRecord;
import com.engine.walpulse.domain.port.out.TransformEnginePort;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Transforms WalChangeRecord instances into JSON envelopes, masking sensitive data and filtering tables.
 */
public class JsonTransformEngineAdapter implements TransformEnginePort {

    private static final Logger log = LoggerFactory.getLogger(JsonTransformEngineAdapter.class);

    private final WalPulseProperties properties;
    private final ObjectMapper objectMapper;

    public JsonTransformEngineAdapter(WalPulseProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Override
    public Optional<SinkRecord> transform(WalChangeRecord record) {
        if (!properties.getFilter().isTableAllowed(record.schemaName(), record.tableName())) {
            log.trace("Table {}.{} excluded by filter rules", record.schemaName(), record.tableName());
            return Optional.empty();
        }

        Map<String, Object> beforeMap = maskAndExtract(record.beforeColumns());
        Map<String, Object> afterMap = maskAndExtract(record.afterColumns());

        String partitionKey = extractPartitionKey(record);
        String destination = properties.getSink().getKafka().getTopicPrefix() +
                record.schemaName() + "." + record.tableName();

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("id", record.transactionId() + ":" + record.lsn().asString());
        envelope.put("op", mapOperationCode(record.operation()));
        envelope.put("schema", record.schemaName());
        envelope.put("table", record.tableName());
        envelope.put("timestamp", record.commitTimestamp() != null ? record.commitTimestamp().toString() : Instant.now().toString());
        envelope.put("lsn", record.lsn().asString());
        envelope.put("txId", record.transactionId());

        if (!beforeMap.isEmpty()) {
            envelope.put("before", beforeMap);
        }
        if (!afterMap.isEmpty()) {
            envelope.put("after", afterMap);
        }

        try {
            String jsonPayload = objectMapper.writeValueAsString(envelope);
            Map<String, String> headers = Map.of(
                    "source", "walpulse",
                    "operation", record.operation().name(),
                    "table", record.fullTableName(),
                    "lsn", record.lsn().asString()
            );

            SinkRecord sinkRecord = new SinkRecord(
                    record.transactionId() + ":" + record.lsn().asString(),
                    destination,
                    partitionKey,
                    jsonPayload,
                    headers,
                    record.lsn(),
                    record.commitTimestamp() != null ? record.commitTimestamp() : Instant.now(),
                    record.operation()
            );

            return Optional.of(sinkRecord);

        } catch (JsonProcessingException e) {
            log.error("Failed to serialize change record to JSON envelope", e);
            return Optional.empty();
        }
    }

    private Map<String, Object> maskAndExtract(java.util.List<ColumnValue> columns) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (columns == null) {
            return map;
        }

        for (ColumnValue col : columns) {
            if (properties.getFilter().isFieldMasked(col.name())) {
                map.put(col.name(), "[REDACTED]");
            } else {
                map.put(col.name(), col.value());
            }
        }
        return map;
    }

    private String extractPartitionKey(WalChangeRecord record) {
        for (ColumnValue col : record.afterColumns()) {
            if (col.isKey() && col.value() != null) {
                return col.value().toString();
            }
        }
        for (ColumnValue col : record.beforeColumns()) {
            if (col.isKey() && col.value() != null) {
                return col.value().toString();
            }
        }
        return record.fullTableName();
    }

    private String mapOperationCode(ChangeType operation) {
        return switch (operation) {
            case INSERT -> "c"; // create
            case UPDATE -> "u"; // update
            case DELETE -> "d"; // delete
            case TRUNCATE -> "t"; // truncate
        };
    }
}
