package com.engine.walpulse.infrastructure;

import com.engine.walpulse.application.dto.WalPulseProperties;
import com.engine.walpulse.domain.event.ChangeType;
import com.engine.walpulse.domain.model.ColumnValue;
import com.engine.walpulse.domain.model.LsnPosition;
import com.engine.walpulse.domain.model.SinkRecord;
import com.engine.walpulse.domain.model.WalChangeRecord;
import com.engine.walpulse.infrastructure.adapter.out.transform.JsonTransformEngineAdapter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class JsonTransformEngineAdapterTest {

    private JsonTransformEngineAdapter transformer;
    private WalPulseProperties properties;

    @BeforeEach
    void setUp() {
        properties = new WalPulseProperties();
        properties.getFilter().setIncludeTables(List.of("public.*"));
        properties.getFilter().setExcludeTables(List.of("public.schema_migrations"));
        properties.getFilter().setMaskedFields(List.of("password", "token"));
        properties.getSink().getKafka().setTopicPrefix("cdc.");

        transformer = new JsonTransformEngineAdapter(properties, new ObjectMapper());
    }

    @Test
    @DisplayName("Should transform allowed table record and mask sensitive fields")
    void testTransformAndMask() {
        WalChangeRecord record = new WalChangeRecord(
                1001L,
                Instant.now(),
                LsnPosition.valueOf("0/16B4FC0"),
                ChangeType.INSERT,
                "public",
                "users",
                List.of(),
                List.of(
                        ColumnValue.of("id", 23, "int4", "42", true),
                        ColumnValue.of("username", 25, "text", "alexander", false),
                        ColumnValue.of("password", 25, "text", "super_secret_123", false)
                ),
                java.util.Map.of()
        );

        Optional<SinkRecord> sinkRecordOpt = transformer.transform(record);
        assertThat(sinkRecordOpt).isPresent();

        SinkRecord sinkRecord = sinkRecordOpt.get();
        assertThat(sinkRecord.destination()).isEqualTo("cdc.public.users");
        assertThat(sinkRecord.partitionKey()).isEqualTo("42");
        assertThat(sinkRecord.payloadJson()).contains("\"username\":\"alexander\"");
        assertThat(sinkRecord.payloadJson()).contains("\"password\":\"[REDACTED]\"");
        assertThat(sinkRecord.payloadJson()).doesNotContain("super_secret_123");
    }

    @Test
    @DisplayName("Should filter out tables matching exclude list")
    void testExcludeTableFilter() {
        WalChangeRecord record = new WalChangeRecord(
                1002L,
                Instant.now(),
                LsnPosition.valueOf("0/16B4FD0"),
                ChangeType.INSERT,
                "public",
                "schema_migrations",
                List.of(),
                List.of(ColumnValue.of("version", 23, "int4", "1", true)),
                java.util.Map.of()
        );

        Optional<SinkRecord> sinkRecordOpt = transformer.transform(record);
        assertThat(sinkRecordOpt).isEmpty();
    }
}
