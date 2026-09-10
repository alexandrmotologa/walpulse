package com.engine.walpulse.application.service;

import com.engine.walpulse.domain.model.TableMetadata;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Thread-safe in-memory cache for PostgreSQL table schemas and schema evolution history.
 */
public class SchemaCacheService {

    private final Map<Integer, TableMetadata> cacheByOid = new ConcurrentHashMap<>();
    private final Map<String, TableMetadata> cacheByName = new ConcurrentHashMap<>();
    private final List<SchemaChangeEntry> evolutionLog = new CopyOnWriteArrayList<>();

    public void registerRelation(TableMetadata metadata) {
        if (metadata == null) return;

        String fullName = metadata.fullTableName();
        TableMetadata existing = cacheByName.get(fullName);

        if (existing != null && !areColumnsEqual(existing.columns(), metadata.columns())) {
            String desc = detectChangeDescription(existing, metadata);
            evolutionLog.add(new SchemaChangeEntry(
                    metadata.schemaName(),
                    metadata.tableName(),
                    Instant.now(),
                    desc,
                    existing.columns(),
                    metadata.columns()
            ));
        } else if (existing == null) {
            evolutionLog.add(new SchemaChangeEntry(
                    metadata.schemaName(),
                    metadata.tableName(),
                    Instant.now(),
                    "Initial table schema registered (" + metadata.columns().size() + " columns)",
                    List.of(),
                    metadata.columns()
            ));
        }

        cacheByOid.put(metadata.relationOid(), metadata);
        cacheByName.put(fullName, metadata);
    }

    public Optional<TableMetadata> getRelation(int relationOid) {
        return Optional.ofNullable(cacheByOid.get(relationOid));
    }

    public Optional<TableMetadata> getRelationByName(String schema, String table) {
        return Optional.ofNullable(cacheByName.get(schema + "." + table));
    }

    public List<TableMetadata> getAllRelations() {
        return new ArrayList<>(cacheByName.values());
    }

    public List<SchemaChangeEntry> getEvolutionLog() {
        List<SchemaChangeEntry> list = new ArrayList<>(evolutionLog);
        Collections.reverse(list);
        return list;
    }

    public void clear() {
        cacheByOid.clear();
        cacheByName.clear();
        evolutionLog.clear();
    }

    public int size() {
        return cacheByName.size();
    }

    private boolean areColumnsEqual(List<TableMetadata.ColumnDefinition> a, List<TableMetadata.ColumnDefinition> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            TableMetadata.ColumnDefinition colA = a.get(i);
            TableMetadata.ColumnDefinition colB = b.get(i);
            if (!Objects.equals(colA.name(), colB.name()) || colA.typeOid() != colB.typeOid() || colA.isKey() != colB.isKey()) {
                return false;
            }
        }
        return true;
    }

    private String detectChangeDescription(TableMetadata oldMeta, TableMetadata newMeta) {
        int oldCols = oldMeta.columns().size();
        int newCols = newMeta.columns().size();

        if (newCols > oldCols) {
            List<String> added = new ArrayList<>();
            for (var col : newMeta.columns()) {
                if (oldMeta.findColumn(col.name()).isEmpty()) {
                    added.add(col.name());
                }
            }
            return "Columns added: [" + String.join(", ", added) + "]";
        } else if (newCols < oldCols) {
            return "Columns dropped (count changed from " + oldCols + " to " + newCols + ")";
        } else {
            return "Column types or primary key definitions modified";
        }
    }

    public record SchemaChangeEntry(
            String schemaName,
            String tableName,
            Instant timestamp,
            String description,
            List<TableMetadata.ColumnDefinition> previousColumns,
            List<TableMetadata.ColumnDefinition> newColumns
    ) {}
}
