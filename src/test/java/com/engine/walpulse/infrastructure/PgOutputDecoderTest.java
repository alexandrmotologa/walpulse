package com.engine.walpulse.infrastructure;

import com.engine.walpulse.application.service.SchemaCacheService;
import com.engine.walpulse.domain.event.ChangeType;
import com.engine.walpulse.domain.event.ReplicationEvent;
import com.engine.walpulse.domain.model.LsnPosition;
import com.engine.walpulse.domain.model.TableMetadata;
import com.engine.walpulse.domain.model.WalChangeRecord;
import com.engine.walpulse.infrastructure.adapter.out.postgres.PgOutputDecoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class PgOutputDecoderTest {

    private PgOutputDecoder decoder;
    private SchemaCacheService schemaCache;

    @BeforeEach
    void setUp() {
        decoder = new PgOutputDecoder();
        schemaCache = new SchemaCacheService();
    }

    @Test
    @DisplayName("Should decode 'B' (Begin) message correctly")
    void testDecodeBegin() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        buf.put((byte) 'B');
        buf.putLong(0x16B4F80L); // final LSN
        buf.putLong(1000000L);   // commit timestamp micros
        buf.putInt(1042);        // XID
        buf.flip();

        ReplicationEvent event = decoder.decode(buf, schemaCache, LsnPosition.valueOf("0/16B4F80"));
        assertThat(event).isInstanceOf(ReplicationEvent.TransactionBegin.class);
        ReplicationEvent.TransactionBegin begin = (ReplicationEvent.TransactionBegin) event;
        assertThat(begin.xid()).isEqualTo(1042L);
        assertThat(begin.lsn().asLong()).isEqualTo(0x16B4F80L);
    }

    @Test
    @DisplayName("Should decode 'R' (Relation) message and register schema in cache")
    void testDecodeRelation() {
        ByteBuffer buf = ByteBuffer.allocate(128);
        buf.put((byte) 'R');
        buf.putInt(16384); // relation OID
        buf.put("public\0".getBytes(StandardCharsets.UTF_8));
        buf.put("orders\0".getBytes(StandardCharsets.UTF_8));
        buf.put((byte) 'd'); // replica identity default
        buf.putShort((short) 2); // 2 columns

        // Column 1: id (int4, key=true)
        buf.put((byte) 1); // flag isKey
        buf.put("id\0".getBytes(StandardCharsets.UTF_8));
        buf.putInt(23); // type OID int4
        buf.putInt(-1); // typmod

        // Column 2: customer_id (text, key=false)
        buf.put((byte) 0); // flag isKey
        buf.put("customer_id\0".getBytes(StandardCharsets.UTF_8));
        buf.putInt(25); // type OID text
        buf.putInt(-1); // typmod
        buf.flip();

        ReplicationEvent event = decoder.decode(buf, schemaCache, LsnPosition.valueOf("0/16B4F90"));
        assertThat(event).isInstanceOf(ReplicationEvent.RelationSchema.class);
        ReplicationEvent.RelationSchema rel = (ReplicationEvent.RelationSchema) event;
        TableMetadata meta = rel.metadata();

        assertThat(meta.relationOid()).isEqualTo(16384);
        assertThat(meta.schemaName()).isEqualTo("public");
        assertThat(meta.tableName()).isEqualTo("orders");
        assertThat(meta.columns()).hasSize(2);
        assertThat(meta.columns().get(0).name()).isEqualTo("id");
        assertThat(meta.columns().get(0).isKey()).isTrue();
        assertThat(meta.columns().get(1).name()).isEqualTo("customer_id");
        assertThat(meta.columns().get(1).isKey()).isFalse();

        schemaCache.registerRelation(meta);
        assertThat(schemaCache.getRelation(16384)).isPresent();
    }

    @Test
    @DisplayName("Should decode 'I' (Insert) message using cached relation metadata")
    void testDecodeInsert() {
        // Register relation first
        TableMetadata.ColumnDefinition col1 = new TableMetadata.ColumnDefinition(0, "id", 23, -1, true);
        TableMetadata.ColumnDefinition col2 = new TableMetadata.ColumnDefinition(1, "status", 25, -1, false);
        TableMetadata metadata = new TableMetadata(16384, "public", "orders", 'd', java.util.List.of(col1, col2));
        schemaCache.registerRelation(metadata);

        // Build Insert packet
        ByteBuffer buf = ByteBuffer.allocate(128);
        buf.put((byte) 'I');
        buf.putInt(16384); // relation OID
        buf.put((byte) 'N'); // new tuple
        buf.putShort((short) 2); // 2 columns

        // Col 1: text "101"
        buf.put((byte) 't');
        byte[] idBytes = "101".getBytes(StandardCharsets.UTF_8);
        buf.putInt(idBytes.length);
        buf.put(idBytes);

        // Col 2: text "PAID"
        buf.put((byte) 't');
        byte[] statusBytes = "PAID".getBytes(StandardCharsets.UTF_8);
        buf.putInt(statusBytes.length);
        buf.put(statusBytes);
        buf.flip();

        LsnPosition lsn = LsnPosition.valueOf("0/16B4FA0");
        ReplicationEvent event = decoder.decode(buf, schemaCache, lsn);

        assertThat(event).isInstanceOf(ReplicationEvent.DataChange.class);
        WalChangeRecord record = ((ReplicationEvent.DataChange) event).record();

        assertThat(record.operation()).isEqualTo(ChangeType.INSERT);
        assertThat(record.fullTableName()).isEqualTo("public.orders");
        assertThat(record.lsn()).isEqualTo(lsn);
        assertThat(record.afterColumns()).hasSize(2);
        assertThat(record.afterColumns().get(0).name()).isEqualTo("id");
        assertThat(record.afterColumns().get(0).value()).isEqualTo("101");
        assertThat(record.afterColumns().get(1).name()).isEqualTo("status");
        assertThat(record.afterColumns().get(1).value()).isEqualTo("PAID");
    }

    @Test
    @DisplayName("Should decode 'C' (Commit) message")
    void testDecodeCommit() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        buf.put((byte) 'C');
        buf.put((byte) 0); // flags
        buf.putLong(0x16B4FB0L); // commit LSN
        buf.putLong(0x16B4FB8L); // end LSN
        buf.putLong(2000000L);   // commit timestamp micros
        buf.flip();

        ReplicationEvent event = decoder.decode(buf, schemaCache, LsnPosition.valueOf("0/16B4FB0"));
        assertThat(event).isInstanceOf(ReplicationEvent.TransactionCommit.class);
        ReplicationEvent.TransactionCommit commit = (ReplicationEvent.TransactionCommit) event;
        assertThat(commit.commit().commitLsn().asLong()).isEqualTo(0x16B4FB0L);
        assertThat(commit.commit().endLsn().asLong()).isEqualTo(0x16B4FB8L);
    }
}
