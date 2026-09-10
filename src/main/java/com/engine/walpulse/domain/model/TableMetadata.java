package com.engine.walpulse.domain.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Cached schema metadata for a PostgreSQL relation captured via 'R' (Relation) messages.
 */
public record TableMetadata(
        int relationOid,
        String schemaName,
        String tableName,
        char replicaIdentity,
        List<ColumnDefinition> columns
) {
    public TableMetadata {
        Objects.requireNonNull(schemaName, "schemaName must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        columns = columns == null ? List.of() : Collections.unmodifiableList(columns);
    }

    public String fullTableName() {
        return schemaName + "." + tableName;
    }

    public Optional<ColumnDefinition> findColumn(String name) {
        return columns.stream()
                .filter(col -> col.name().equalsIgnoreCase(name))
                .findFirst();
    }

    public List<ColumnDefinition> keyColumns() {
        return columns.stream()
                .filter(ColumnDefinition::isKey)
                .toList();
    }

    /**
     * Definition of an individual column within relation metadata.
     */
    public record ColumnDefinition(
            int ordinal,
            String name,
            int typeOid,
            int typeModifier,
            boolean isKey
    ) {}
}
