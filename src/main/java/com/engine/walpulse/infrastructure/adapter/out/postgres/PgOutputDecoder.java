package com.engine.walpulse.infrastructure.adapter.out.postgres;

import com.engine.walpulse.application.service.SchemaCacheService;
import com.engine.walpulse.domain.event.ChangeType;
import com.engine.walpulse.domain.event.ReplicationEvent;
import com.engine.walpulse.domain.event.StreamCommitEvent;
import com.engine.walpulse.domain.model.ColumnValue;
import com.engine.walpulse.domain.model.LsnPosition;
import com.engine.walpulse.domain.model.TableMetadata;
import com.engine.walpulse.domain.model.WalChangeRecord;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Binary protocol decoder for PostgreSQL's native pgoutput logical replication plugin.
 */
public class PgOutputDecoder {

    // Postgres epoch is 2000-01-01 00:00:00 UTC
    private static final Instant PG_EPOCH = Instant.parse("2000-01-01T00:00:00Z");

    private long currentXid = 0;
    private Instant currentCommitTimestamp = Instant.EPOCH;

    /**
     * Decodes a raw binary buffer into a domain ReplicationEvent.
     *
     * @param buffer      the byte buffer from the replication stream
     * @param schemaCache cache used to look up column names and types for data messages
     * @param packetLsn   the LSN position of this replication packet
     * @return ReplicationEvent
     */
    public ReplicationEvent decode(ByteBuffer buffer, SchemaCacheService schemaCache, LsnPosition packetLsn) {
        if (!buffer.hasRemaining()) {
            return new ReplicationEvent.Ignored(' ', "Empty buffer");
        }

        char messageType = (char) buffer.get();

        return switch (messageType) {
            case 'B' -> decodeBegin(buffer);
            case 'C' -> decodeCommit(buffer);
            case 'R' -> decodeRelation(buffer);
            case 'I' -> decodeInsert(buffer, schemaCache, packetLsn);
            case 'U' -> decodeUpdate(buffer, schemaCache, packetLsn);
            case 'D' -> decodeDelete(buffer, schemaCache, packetLsn);
            case 'T' -> decodeTruncate(buffer);
            case 'Y' -> decodeType(buffer);
            default -> new ReplicationEvent.Ignored(messageType, "Unhandled message type: " + messageType);
        };
    }

    private ReplicationEvent decodeBegin(ByteBuffer buffer) {
        long finalLsn = buffer.getLong();
        long commitTimestampMicros = buffer.getLong();
        int xid = buffer.getInt();

        this.currentXid = Integer.toUnsignedLong(xid);
        this.currentCommitTimestamp = PG_EPOCH.plus(commitTimestampMicros, ChronoUnit.MICROS);

        return new ReplicationEvent.TransactionBegin(currentXid, currentCommitTimestamp, LsnPosition.fromLong(finalLsn));
    }

    private ReplicationEvent decodeCommit(ByteBuffer buffer) {
        buffer.get(); // flags
        long commitLsn = buffer.getLong();
        long endLsn = buffer.getLong();
        long commitTimestampMicros = buffer.getLong();

        Instant commitTimestamp = PG_EPOCH.plus(commitTimestampMicros, ChronoUnit.MICROS);
        StreamCommitEvent event = new StreamCommitEvent(
                currentXid,
                LsnPosition.fromLong(commitLsn),
                LsnPosition.fromLong(endLsn),
                commitTimestamp
        );

        return new ReplicationEvent.TransactionCommit(event);
    }

    private ReplicationEvent decodeRelation(ByteBuffer buffer) {
        int relationOid = buffer.getInt();
        String schemaName = readNullTerminatedString(buffer);
        String tableName = readNullTerminatedString(buffer);
        char replicaIdentity = (char) buffer.get();
        short columnCount = buffer.getShort();

        List<TableMetadata.ColumnDefinition> columns = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            byte flags = buffer.get();
            boolean isKey = (flags & 1) != 0;
            String colName = readNullTerminatedString(buffer);
            int typeOid = buffer.getInt();
            int typeMod = buffer.getInt();

            columns.add(new TableMetadata.ColumnDefinition(i, colName, typeOid, typeMod, isKey));
        }

        TableMetadata metadata = new TableMetadata(relationOid, schemaName, tableName, replicaIdentity, columns);
        return new ReplicationEvent.RelationSchema(metadata);
    }

    private ReplicationEvent decodeInsert(ByteBuffer buffer, SchemaCacheService schemaCache, LsnPosition packetLsn) {
        int relationOid = buffer.getInt();
        char tupleType = (char) buffer.get(); // Should be 'N'

        Optional<TableMetadata> metadataOpt = schemaCache.getRelation(relationOid);
        if (metadataOpt.isEmpty()) {
            return new ReplicationEvent.Ignored('I', "Unknown relation OID: " + relationOid);
        }

        TableMetadata metadata = metadataOpt.get();
        List<ColumnValue> columns = decodeTupleData(buffer, metadata);

        WalChangeRecord record = new WalChangeRecord(
                currentXid,
                currentCommitTimestamp,
                packetLsn,
                ChangeType.INSERT,
                metadata.schemaName(),
                metadata.tableName(),
                List.of(),
                columns,
                Map.of("relationOid", String.valueOf(relationOid))
        );

        return new ReplicationEvent.DataChange(record);
    }

    private ReplicationEvent decodeUpdate(ByteBuffer buffer, SchemaCacheService schemaCache, LsnPosition packetLsn) {
        int relationOid = buffer.getInt();

        Optional<TableMetadata> metadataOpt = schemaCache.getRelation(relationOid);
        if (metadataOpt.isEmpty()) {
            return new ReplicationEvent.Ignored('U', "Unknown relation OID: " + relationOid);
        }

        TableMetadata metadata = metadataOpt.get();
        List<ColumnValue> beforeColumns = List.of();

        char subType = (char) buffer.get();
        if (subType == 'K' || subType == 'O') {
            beforeColumns = decodeTupleData(buffer, metadata);
            subType = (char) buffer.get();
        }

        List<ColumnValue> afterColumns = List.of();
        if (subType == 'N') {
            afterColumns = decodeTupleData(buffer, metadata);
        }

        WalChangeRecord record = new WalChangeRecord(
                currentXid,
                currentCommitTimestamp,
                packetLsn,
                ChangeType.UPDATE,
                metadata.schemaName(),
                metadata.tableName(),
                beforeColumns,
                afterColumns,
                Map.of("relationOid", String.valueOf(relationOid))
        );

        return new ReplicationEvent.DataChange(record);
    }

    private ReplicationEvent decodeDelete(ByteBuffer buffer, SchemaCacheService schemaCache, LsnPosition packetLsn) {
        int relationOid = buffer.getInt();
        buffer.get(); // 'K' or 'O'

        Optional<TableMetadata> metadataOpt = schemaCache.getRelation(relationOid);
        if (metadataOpt.isEmpty()) {
            return new ReplicationEvent.Ignored('D', "Unknown relation OID: " + relationOid);
        }

        TableMetadata metadata = metadataOpt.get();
        List<ColumnValue> beforeColumns = decodeTupleData(buffer, metadata);

        WalChangeRecord record = new WalChangeRecord(
                currentXid,
                currentCommitTimestamp,
                packetLsn,
                ChangeType.DELETE,
                metadata.schemaName(),
                metadata.tableName(),
                beforeColumns,
                List.of(),
                Map.of("relationOid", String.valueOf(relationOid))
        );

        return new ReplicationEvent.DataChange(record);
    }

    private ReplicationEvent decodeTruncate(ByteBuffer buffer) {
        int relationCount = buffer.getInt();
        byte flags = buffer.get();
        boolean cascade = (flags & 1) != 0;

        List<Integer> relationOids = new ArrayList<>(relationCount);
        for (int i = 0; i < relationCount; i++) {
            relationOids.add(buffer.getInt());
        }

        return new ReplicationEvent.TableTruncate(relationOids, cascade);
    }

    private ReplicationEvent decodeType(ByteBuffer buffer) {
        buffer.getInt(); // type OID
        readNullTerminatedString(buffer); // namespace
        readNullTerminatedString(buffer); // name
        return new ReplicationEvent.Ignored('Y', "Type metadata processed");
    }

    private List<ColumnValue> decodeTupleData(ByteBuffer buffer, TableMetadata metadata) {
        short columnCount = buffer.getShort();
        List<ColumnValue> columnValues = new ArrayList<>(columnCount);

        for (int i = 0; i < columnCount; i++) {
            char valueKind = (char) buffer.get();
            TableMetadata.ColumnDefinition colDef = i < metadata.columns().size() ? metadata.columns().get(i) : null;
            String colName = colDef != null ? colDef.name() : "col_" + i;
            int typeOid = colDef != null ? colDef.typeOid() : 0;
            boolean isKey = colDef != null && colDef.isKey();

            switch (valueKind) {
                case 'n' -> columnValues.add(ColumnValue.nullValue(colName, typeOid, mapTypeOid(typeOid), isKey));
                case 'u' -> columnValues.add(ColumnValue.unchangedToast(colName, typeOid, mapTypeOid(typeOid), isKey));
                case 't' -> {
                    int length = buffer.getInt();
                    byte[] bytes = new byte[length];
                    buffer.get(bytes);
                    String textValue = new String(bytes, StandardCharsets.UTF_8);
                    columnValues.add(ColumnValue.of(colName, typeOid, mapTypeOid(typeOid), textValue, isKey));
                }
                case 'b' -> {
                    int length = buffer.getInt();
                    byte[] bytes = new byte[length];
                    buffer.get(bytes);
                    columnValues.add(ColumnValue.of(colName, typeOid, mapTypeOid(typeOid), bytes, isKey));
                }
                default -> columnValues.add(ColumnValue.nullValue(colName, typeOid, "unknown", isKey));
            }
        }

        return columnValues;
    }

    private String readNullTerminatedString(ByteBuffer buffer) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            if (b == 0) {
                break;
            }
            baos.write(b);
        }
        return baos.toString(StandardCharsets.UTF_8);
    }

    private String mapTypeOid(int oid) {
        return switch (oid) {
            case 16 -> "bool";
            case 20 -> "int8";
            case 21 -> "int2";
            case 23 -> "int4";
            case 25 -> "text";
            case 700 -> "float4";
            case 701 -> "float8";
            case 1082 -> "date";
            case 1114 -> "timestamp";
            case 1184 -> "timestamptz";
            case 114 -> "json";
            case 3802 -> "jsonb";
            case 2950 -> "uuid";
            default -> "oid_" + oid;
        };
    }
}
