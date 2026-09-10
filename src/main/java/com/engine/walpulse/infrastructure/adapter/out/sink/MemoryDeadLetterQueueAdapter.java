package com.engine.walpulse.infrastructure.adapter.out.sink;

import com.engine.walpulse.domain.model.DlqRecord;
import com.engine.walpulse.domain.model.SinkRecord;
import com.engine.walpulse.domain.port.out.DeadLetterQueuePort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory thread-safe implementation of DeadLetterQueuePort with bounded capacity.
 */
public class MemoryDeadLetterQueueAdapter implements DeadLetterQueuePort {

    private static final Logger log = LoggerFactory.getLogger(MemoryDeadLetterQueueAdapter.class);
    private static final int MAX_DLQ_CAPACITY = 1000;

    private final Map<String, DlqRecord> records = new ConcurrentHashMap<>();
    private final List<String> order = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void enqueue(SinkRecord record, String errorReason) {
        if (record == null) return;

        String id = record.id() != null ? record.id() : String.valueOf(System.currentTimeMillis());

        if (records.size() >= MAX_DLQ_CAPACITY && !records.containsKey(id)) {
            // Evict oldest
            if (!order.isEmpty()) {
                String oldestId = order.remove(0);
                records.remove(oldestId);
            }
        }

        DlqRecord dlqRecord = records.compute(id, (k, existing) -> {
            if (existing != null) {
                return existing.withIncrementedRetry(errorReason);
            }
            return new DlqRecord(id, record, errorReason, Instant.now(), 0);
        });

        if (!order.contains(id)) {
            order.add(id);
        }

        log.warn("Record {} enqueued to DLQ. Reason: {}", id, errorReason);
    }

    @Override
    public List<DlqRecord> listAll() {
        List<DlqRecord> list = new ArrayList<>();
        synchronized (order) {
            for (int i = order.size() - 1; i >= 0; i--) {
                String id = order.get(i);
                DlqRecord r = records.get(id);
                if (r != null) {
                    list.add(r);
                }
            }
        }
        return list;
    }

    @Override
    public Optional<DlqRecord> findById(String id) {
        if (id == null) return Optional.empty();
        return Optional.ofNullable(records.get(id));
    }

    @Override
    public boolean remove(String id) {
        if (id == null) return false;
        order.remove(id);
        return records.remove(id) != null;
    }

    @Override
    public void clear() {
        records.clear();
        order.clear();
        log.info("Cleared all records from Dead Letter Queue");
    }

    @Override
    public int size() {
        return records.size();
    }
}
