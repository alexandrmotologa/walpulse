# WalPulse

WalPulse is a standalone Change Data Capture (CDC) engine and stream transformer for PostgreSQL. It attaches directly to PostgreSQL logical replication slots using the native `pgoutput` streaming protocol over standard TCP. It captures row-level inserts, updates, and deletes with sub-millisecond latency, performs zero polling on source tables, transforms payloads in flight, and forwards events to Kafka, HTTP webhooks, or log sinks.

Unlike Kafka Connect with Debezium, WalPulse runs as an independent lightweight binary. It requires no JVM cluster coordination, starts in seconds, and uses Java 21 Virtual Threads to handle streaming pipelines with low memory overhead.

## Core capabilities

- Native `pgoutput` logical replication decoder: decodes relation, insert, update, delete, truncate, begin, and commit messages directly from WAL streams.
- Zero polling: receives database changes as they are written to the Write-Ahead Log without running `SELECT` queries or taking table locks.
- Dynamic schema synchronization: updates cached table structures on the fly when schema changes occur in PostgreSQL.
- Stream transformations: filters tables with include/exclude rules, masks sensitive fields before dispatch, and reformats payloads.
- Multi-sink dispatching: delivers events to Apache Kafka, HTTP/2 webhooks, and local logging sinks with at-least-once guarantees.
- Safe LSN tracking: acknowledges flushed WAL positions to PostgreSQL only after the target sink confirms receipt, preventing data loss on restarts.
- Real-time SSE dashboard: includes a web console at `/dashboard` displaying live LSN lag, events per second, and a live inspection feed.
- Hexagonal architecture: keeps the domain model isolated from framework and driver dependencies.

## Architecture

```
com.engine.walpulse
├── domain                         # Pure Java 21 domain, zero framework imports
│   ├── model                      # WalChangeRecord, TableMetadata, ColumnValue, LsnPosition
│   ├── event                      # ChangeType, StreamCommitEvent
│   ├── exception                  # WalStreamException, ReplicationSlotException
│   └── port                       # Inbound use cases and outbound ports
│       ├── in                     # StartReplicationUseCase, StopReplicationUseCase
│       └── out                    # LogicalReplicationPort, TransformEnginePort, EventSinkPort
├── application                    # Orchestration and state services
│   ├── service                    # ReplicationCoordinator, LsnTrackerService, SchemaCacheService
│   └── dto                        # WalPulseConfig, TableFilterRule, SinkRecord
└── infrastructure                 # Drivers, web adapters, and sinks
    ├── adapter
    │   ├── in/rest                # Control plane REST endpoints and SSE stream
    │   ├── out/postgres           # PGReplicationStreamAdapter and PgOutputDecoder
    │   ├── out/transform          # JsonTransformEngineAdapter
    │   └── out/sink               # KafkaSinkAdapter, WebhookSinkAdapter, LoggingSinkAdapter
    ├── config                     # Virtual thread executor and system beans
    └── dashboard                  # Static single-page dashboard assets
```

## Quickstart

### 1. Configure PostgreSQL

PostgreSQL requires `wal_level = logical` to support logical decoding:

```sql
-- In postgresql.conf:
-- wal_level = logical
-- max_replication_slots = 10
-- max_wal_senders = 10

-- Create a dedicated replication user
CREATE ROLE walpulse_user WITH REPLICATION LOGIN PASSWORD 'secret';

-- Grant read privileges on target tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO walpulse_user;

-- Create publication for all tables or specific tables
CREATE PUBLICATION walpulse_pub FOR ALL TABLES;
```

### 2. Configure WalPulse

Configure database connection and sinks in `application.yml` or via environment variables:

```yaml
walpulse:
  postgres:
    host: localhost
    port: 5432
    database: appdb
    username: walpulse_user
    password: secret
    slot-name: walpulse_slot
    publication-name: walpulse_pub
    create-slot-if-missing: true
  filter:
    include-tables:
      - "public.*"
    exclude-tables:
      - "public.schema_migrations"
    masked-fields:
      - "password"
      - "credit_card"
      - "ssn"
  sink:
    type: logging # logging, kafka, webhook, or composite
    kafka:
      bootstrap-servers: localhost:9092
      topic-prefix: "cdc."
    webhook:
      url: "https://api.example.com/webhooks/cdc"
      secret: "webhook-signing-secret"
```

### 3. Build and Run

```bash
# Build with Maven
mvn clean package -DskipTests

# Run application
java -jar target/walpulse-1.0.0.jar
```

Open `http://localhost:8080/dashboard` in a browser to monitor the replication stream.

## REST API and Monitoring

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/status` | GET | Current replication state, active slot, and LSN positions |
| `/api/v1/stream/live` | GET | Server-Sent Events (SSE) feed of decoded WAL records |
| `/api/v1/replication/start` | POST | Starts the replication stream worker |
| `/api/v1/replication/stop` | POST | Stops the replication stream worker gracefully |
| `/actuator/health` | GET | System and replication slot health status |
| `/actuator/prometheus` | GET | Prometheus metrics including processed events and LSN lag |

## Verification and Testing

WalPulse includes automated test suites covering:
- Binary pgoutput frame decoding (Relation, Insert, Update, Delete, Begin, Commit).
- Dynamic JSON field extraction and PII masking.
- ArchUnit tests verifying that the domain layer contains zero framework imports.
- Pipeline coordinator integration with mocked replication streams.

Run tests:

```bash
mvn clean test
```

## License

MIT License. See [LICENSE](LICENSE) for details.
