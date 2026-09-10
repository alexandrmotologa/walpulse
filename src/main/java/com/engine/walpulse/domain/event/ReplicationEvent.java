package com.engine.walpulse.domain.event;

import com.engine.walpulse.domain.model.LsnPosition;
import com.engine.walpulse.domain.model.TableMetadata;
import com.engine.walpulse.domain.model.WalChangeRecord;

import java.time.Instant;
import java.util.List;

/**
 * Sealed interface representing all replication events emitted from the logical replication stream.
 */
public sealed interface ReplicationEvent {

    record DataChange(WalChangeRecord record) implements ReplicationEvent {}

    record RelationSchema(TableMetadata metadata) implements ReplicationEvent {}

    record TransactionCommit(StreamCommitEvent commit) implements ReplicationEvent {}

    record TransactionBegin(long xid, Instant timestamp, LsnPosition lsn) implements ReplicationEvent {}

    record TableTruncate(List<Integer> relationOids, boolean cascade) implements ReplicationEvent {}

    record Heartbeat(LsnPosition serverLsn) implements ReplicationEvent {}

    record Ignored(char typeByte, String reason) implements ReplicationEvent {}
}
