package com.engine.walpulse.infrastructure.adapter.out.postgres;

import com.engine.walpulse.application.dto.WalPulseProperties;
import com.engine.walpulse.application.service.SchemaCacheService;
import com.engine.walpulse.domain.event.ReplicationEvent;
import com.engine.walpulse.domain.exception.ReplicationSlotException;
import com.engine.walpulse.domain.exception.WalStreamException;
import com.engine.walpulse.domain.model.LsnPosition;
import com.engine.walpulse.domain.port.out.LogicalReplicationPort;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * PostgreSQL native logical replication adapter using pgoutput and PGReplicationStream.
 */
public class PGReplicationStreamAdapter implements LogicalReplicationPort {

    private static final Logger log = LoggerFactory.getLogger(PGReplicationStreamAdapter.class);

    private final WalPulseProperties properties;
    private final PgOutputDecoder decoder;
    private final SchemaCacheService schemaCache;

    private Connection connection;
    private PGReplicationStream replicationStream;
    private volatile boolean connected = false;

    private volatile LsnPosition lastReceivedLsn = LsnPosition.ZERO;
    private volatile LsnPosition lastFlushedLsn = LsnPosition.ZERO;

    public PGReplicationStreamAdapter(
            WalPulseProperties properties,
            PgOutputDecoder decoder,
            SchemaCacheService schemaCache
    ) {
        this.properties = properties;
        this.decoder = decoder;
        this.schemaCache = schemaCache;
    }

    @Override
    public synchronized void connect() {
        if (connected) {
            return;
        }

        WalPulseProperties.PostgresProperties pg = properties.getPostgres();
        String url = String.format("jdbc:postgresql://%s:%d/%s", pg.getHost(), pg.getPort(), pg.getDatabase());

        Properties props = new Properties();
        PGProperty.USER.set(props, pg.getUsername());
        PGProperty.PASSWORD.set(props, pg.getPassword());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "10.0");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        try {
            log.info("Connecting to PostgreSQL replication endpoint: {}", url);
            this.connection = DriverManager.getConnection(url, props);
            PGConnection pgConnection = connection.unwrap(PGConnection.class);

            if (pg.isCreateSlotIfMissing()) {
                ensureSlotExists(pgConnection, pg.getSlotName());
            }

            var streamBuilder = pgConnection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(pg.getSlotName())
                    .withSlotOption("proto_version", "1")
                    .withSlotOption("publication_names", pg.getPublicationName())
                    .withStatusInterval(pg.getStatusIntervalMs(), TimeUnit.MILLISECONDS);

            if (lastFlushedLsn.compareTo(LsnPosition.ZERO) > 0) {
                streamBuilder.withStartPosition(LogSequenceNumber.valueOf(lastFlushedLsn.asLong()));
            }

            this.replicationStream = streamBuilder.start();
            this.connected = true;
            log.info("PostgreSQL replication stream attached to slot '{}'", pg.getSlotName());

        } catch (SQLException e) {
            this.connected = false;
            throw new WalStreamException("Failed to establish PostgreSQL replication stream: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<ReplicationEvent> readNextEvent() {
        if (!connected || replicationStream == null) {
            return Optional.empty();
        }

        try {
            ByteBuffer buffer = replicationStream.readPending();
            if (buffer != null) {
                LogSequenceNumber serverLsn = replicationStream.getLastReceiveLSN();
                if (serverLsn != null) {
                    this.lastReceivedLsn = LsnPosition.fromLong(serverLsn.asLong());
                }
                ReplicationEvent event = decoder.decode(buffer, schemaCache, this.lastReceivedLsn);
                return Optional.of(event);
            }
            return Optional.empty();
        } catch (SQLException e) {
            log.error("Error reading replication stream packet", e);
            connected = false;
            throw new WalStreamException("Error reading replication packet: " + e.getMessage(), e);
        }
    }

    @Override
    public void updateFlushedLsn(LsnPosition lsn) {
        if (lsn == null || !connected || replicationStream == null) {
            return;
        }

        try {
            this.lastFlushedLsn = lsn;
            LogSequenceNumber pgLsn = LogSequenceNumber.valueOf(lsn.asLong());
            replicationStream.setFlushedLSN(pgLsn);
            replicationStream.setAppliedLSN(pgLsn);
            replicationStream.forceUpdateStatus();
        } catch (SQLException e) {
            log.warn("Failed to update flushed LSN feedback to PostgreSQL", e);
        }
    }

    @Override
    public LsnPosition getLastReceivedLsn() {
        return lastReceivedLsn;
    }

    @Override
    public LsnPosition getLastFlushedLsn() {
        return lastFlushedLsn;
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public synchronized void close() {
        connected = false;
        if (replicationStream != null) {
            try {
                replicationStream.close();
            } catch (SQLException ignored) {}
            replicationStream = null;
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {}
            connection = null;
        }
        log.info("PostgreSQL replication stream closed");
    }

    private void ensureSlotExists(PGConnection pgConnection, String slotName) {
        try (var stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SELECT 1 FROM pg_replication_slots WHERE slot_name = '" + slotName + "'");
            if (!rs.next()) {
                log.info("Creating replication slot '{}' with plugin 'pgoutput'", slotName);
                pgConnection.getReplicationAPI()
                        .createReplicationSlot()
                        .logical()
                        .withSlotName(slotName)
                        .withOutputPlugin("pgoutput")
                        .make();
            }
        } catch (SQLException e) {
            // Error code 42710 means duplicate_object (slot already exists)
            if (!"42710".equals(e.getSQLState())) {
                throw new ReplicationSlotException(slotName, "Could not verify or create slot: " + e.getMessage(), e);
            }
        }
    }
}
