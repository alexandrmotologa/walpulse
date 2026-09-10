package com.engine.walpulse.domain.model;

import com.engine.walpulse.domain.event.ChangeType;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Encapsulates a row-level Change Data Capture (CDC) event emitted from the WAL stream.
 */
public record WalChangeRecord(
        long transactionId,
        Instant commitTimestamp,
        LsnPosition lsn,
        ChangeType operation,
        String schemaName,
        String tableName,
        List<ColumnValue> beforeColumns,
        List<ColumnValue> afterColumns,
        Map<String, String> metadata
) {
    public WalChangeRecord {
        Objects.requireNonNull(lsn, "lsn must not be null");
        Objects.requireNonNull(operation, "operation must not be null");
        Objects.requireNonNull(schemaName, "schemaName must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        beforeColumns = beforeColumns == null ? List.of() : Collections.unmodifiableList(beforeColumns);
        afterColumns = afterColumns == null ? List.of() : Collections.unmodifiableList(afterColumns);
        metadata = metadata == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
    }

    public String fullTableName() {
        return schemaName + "." + tableName;
    }

    public Optional<ColumnValue> findAfterColumn(String columnName) {
        return afterColumns.stream()
                .filter(col -> col.name().equalsIgnoreCase(columnName))
                .findFirst();
    }

    public Optional<ColumnValue> findBeforeColumn(String columnName) {
        return beforeColumns.stream()
                .filter(col -> col.name().equalsIgnoreCase(columnName))
                .findFirst();
    }

    public Map<String, Object> afterMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        for (ColumnValue col : afterColumns) {
            map.put(col.name(), col.value());
        }
        return map;
    }

    public Map<String, Object> beforeMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        for (ColumnValue col : beforeColumns) {
            map.put(col.name(), col.value());
        }
        return map;
    }
}
