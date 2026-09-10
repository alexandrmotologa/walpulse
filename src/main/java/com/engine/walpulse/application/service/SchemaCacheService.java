package com.engine.walpulse.application.service;

import com.engine.walpulse.domain.model.TableMetadata;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory cache for PostgreSQL table schemas captured from 'R' (Relation) messages.
 */
public class SchemaCacheService {

    private final Map<Integer, TableMetadata> cache = new ConcurrentHashMap<>();

    public void registerRelation(TableMetadata metadata) {
        if (metadata != null) {
            cache.put(metadata.relationOid(), metadata);
        }
    }

    public Optional<TableMetadata> getRelation(int relationOid) {
        return Optional.ofNullable(cache.get(relationOid));
    }

    public void clear() {
        cache.clear();
    }

    public int size() {
        return cache.size();
    }
}
