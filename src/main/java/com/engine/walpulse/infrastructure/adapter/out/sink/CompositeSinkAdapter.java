package com.engine.walpulse.infrastructure.adapter.out.sink;

import com.engine.walpulse.domain.model.SinkRecord;
import com.engine.walpulse.domain.port.out.EventSinkPort;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Fan-out composite sink dispatcher that broadcasts records to multiple sinks in parallel.
 */
public class CompositeSinkAdapter implements EventSinkPort {

    private final List<EventSinkPort> sinks;

    public CompositeSinkAdapter(List<EventSinkPort> sinks) {
        this.sinks = sinks != null ? List.copyOf(sinks) : List.of();
    }

    @Override
    public CompletableFuture<Void> send(SinkRecord record) {
        if (sinks.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        List<CompletableFuture<Void>> futures = sinks.stream()
                .map(sink -> sink.send(record))
                .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    @Override
    public String getSinkName() {
        return "composite[" + String.join(",", sinks.stream().map(EventSinkPort::getSinkName).toList()) + "]";
    }
}
