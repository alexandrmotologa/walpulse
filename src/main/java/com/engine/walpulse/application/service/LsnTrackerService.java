package com.engine.walpulse.application.service;

import com.engine.walpulse.domain.model.LsnPosition;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks received, processed, and acknowledged LSN positions with thread-safe atomic watermarks.
 */
public class LsnTrackerService {

    private final AtomicReference<LsnPosition> lastReceived = new AtomicReference<>(LsnPosition.ZERO);
    private final AtomicReference<LsnPosition> lastFlushed = new AtomicReference<>(LsnPosition.ZERO);
    private final AtomicLong totalEventsProcessed = new AtomicLong(0);
    private final AtomicLong totalTransactionsCommitted = new AtomicLong(0);

    public void onLsnReceived(LsnPosition lsn) {
        if (lsn != null) {
            lastReceived.accumulateAndGet(lsn, (cur, next) -> next.compareTo(cur) > 0 ? next : cur);
        }
    }

    public void onLsnFlushed(LsnPosition lsn) {
        if (lsn != null) {
            lastFlushed.accumulateAndGet(lsn, (cur, next) -> next.compareTo(cur) > 0 ? next : cur);
        }
    }

    public void incrementEvents() {
        totalEventsProcessed.incrementAndGet();
    }

    public void incrementCommits() {
        totalTransactionsCommitted.incrementAndGet();
    }

    public LsnPosition getLastReceivedLsn() {
        return lastReceived.get();
    }

    public LsnPosition getLastFlushedLsn() {
        return lastFlushed.get();
    }

    public long getLagBytes() {
        LsnPosition received = lastReceived.get();
        LsnPosition flushed = lastFlushed.get();
        return received.distanceTo(flushed);
    }

    public long getTotalEventsProcessed() {
        return totalEventsProcessed.get();
    }

    public long getTotalTransactionsCommitted() {
        return totalTransactionsCommitted.get();
    }

    public void reset() {
        lastReceived.set(LsnPosition.ZERO);
        lastFlushed.set(LsnPosition.ZERO);
        totalEventsProcessed.set(0);
        totalTransactionsCommitted.set(0);
    }
}
