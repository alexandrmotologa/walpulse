# WalPulse

WalPulse is a standalone Change Data Capture (CDC) engine and stream transformer for PostgreSQL. It attaches directly to PostgreSQL logical replication slots using the native `pgoutput` streaming protocol over standard TCP. It captures row-level inserts, updates, and deletes with sub-millisecond latency, performs zero polling on source tables, transforms payloads in flight, and forwards events to Kafka, HTTP webhooks, or log sinks.

Unlike Kafka Connect with Debezium, WalPulse runs as an independent lightweight binary. It requires no JVM cluster coordination, starts in seconds, and uses Java 21 Virtual Threads to handle streaming pipelines with low memory overhead.

## Core capabilities

- Native `pgoutput` logical replication decoder: decodes relation, insert, update, delete, truncate, begin, and commit messages directly from WAL streams.
- Zero polling: receives database changes as they are written to the Write-Ahead Log without running `SELECT` queries or taking table locks.
- Dynamic schema synchronization and catalog: updates cached table structures on the fly when schema changes occur in PostgreSQL, retaining an evolution timeline.
- Transactional outbox router: detects outbox pattern tables, unpacks JSON event payloads, and dynamically routes records to custom destination topics.
- Dead Letter Queue with redrive: diverts failed sink deliveries into an inspectable DLQ, preventing replication stalls while providing manual or automated redrive.
- Stream transformations: filters tables with include/exclude rules, masks sensitive fields before dispatch, and reformats payloads.
- Multi-sink dispatching: delivers events to Apache Kafka, HTTP/2 webhooks, and local logging sinks with at-least-once guarantees.
- Safe LSN tracking: acknowledges flushed WAL positions to PostgreSQL only after the target sink confirms receipt, preventing data loss on restarts.
- Interactive simulation studio: generates synthetic orders, payment updates, customer deletions, outbox messages, and traffic bursts directly from the UI for testing without a live database.
- Real-time SSE dashboard: includes a web console at `/dashboard` displaying live LSN lag, events per second, schema catalog, DLQ studio, and a rule testing sandbox.
- Hexagonal architecture: keeps the domain model isolated from framework and driver dependencies.

## Architecture

```
com.engine.walpulse
├── domain                         # Pure Java 21 domain, zero framework imports
│   ├── model                      # WalChangeRecord, TableMetadata, ColumnValue, LsnPosition, DlqRecord, SinkRecord
│   ├── event                      # ChangeType, StreamCommitEvent, ReplicationEvent
│   ├── exception                  # WalStreamException, ReplicationSlotException
│   └── port                       # Inbound use cases and outbound ports
│       ├── in                     # StartReplicationUseCase, StopReplicationUseCase, RedriveDlqUseCase
│       └── out                    # LogicalReplicationPort, TransformEnginePort, EventSinkPort, DeadLetterQueuePort
├── application                    # Orchestration and state services
│   ├── service                    # ReplicationCoordinator, LsnTrackerService, SchemaCacheService, WalStreamSimulator
│   └── dto                        # WalPulseProperties, TableFilterRule
└── infrastructure                 # Drivers, web adapters, and sinks
    ├── adapter
    │   ├── in/rest                # ControlPlaneController, DlqController, SchemaController, SimulationController, RulesConfigController
    │   ├── out/postgres           # PGReplicationStreamAdapter and PgOutputDecoder
    │   ├── out/transform          # JsonTransformEngineAdapter (masking, outbox routing)
    │   └── out/sink               # KafkaSinkAdapter, WebhookSinkAdapter, LoggingSinkAdapter, MemoryDeadLetterQueueAdapter
    ├── config                     # System configuration beans
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

Open `http://localhost:8080/dashboard` in a browser to monitor the replication stream or launch synthetic traffic in the Simulation Studio.

## REST API reference

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/status` | GET | Current replication state, active slot, and LSN positions |
| `/api/v1/stream/live` | GET | Server-Sent Events (SSE) feed of decoded WAL records |
| `/api/v1/replication/start` | POST | Starts the replication stream worker |
| `/api/v1/replication/stop` | POST | Stops the replication stream worker gracefully |
| `/api/v1/schemas` | GET | Lists all tables and column metadata discovered in the replication stream |
| `/api/v1/schemas/history` | GET | Lists detected schema evolution events (`ALTER TABLE`) |
| `/api/v1/dlq` | GET | Lists all failed deliveries stored in the Dead Letter Queue |
| `/api/v1/dlq/{id}/redrive` | POST | Attempts redelivery of a specific DLQ message to its sink |
| `/api/v1/dlq/redrive-all` | POST | Attempts redelivery for all pending DLQ records |
| `/api/v1/dlq/{id}` | DELETE | Deletes a record from the DLQ |
| `/api/v1/dlq` | DELETE | Clears the DLQ |
| `/api/v1/simulate/order` | POST | Simulates an `INSERT` event into `public.orders` |
| `/api/v1/simulate/payment` | POST | Simulates an `UPDATE` event on an order |
| `/api/v1/simulate/delete` | POST | Simulates a `DELETE` event on a customer |
| `/api/v1/simulate/outbox` | POST | Simulates an outbox event routed to a dynamic topic |
| `/api/v1/simulate/schema` | POST | Simulates schema evolution by adding a new column |
| `/api/v1/simulate/burst` | POST | Launches a burst of concurrent events on a Virtual Thread |
| `/api/v1/config/rules` | GET/PUT | Reads or updates table filtering and PII masking rules at runtime |
| `/api/v1/config/sandbox` | POST | Tests field masking and table filtering on sample JSON payloads |
| `/actuator/health` | GET | System and replication slot health status |
| `/actuator/prometheus` | GET | Prometheus metrics including processed events and LSN lag |

## Verification and Testing

WalPulse includes automated test suites covering:
- Binary pgoutput frame decoding (Relation, Insert, Update, Delete, Begin, Commit).
- Dynamic JSON field extraction, PII masking, and Transactional Outbox routing.
- Dead Letter Queue routing and redrive lifecycle.
- Schema evolution tracking and cache invalidation.
- ArchUnit tests verifying that the domain layer contains zero framework imports.
- Pipeline coordinator integration with mocked replication streams.

Run tests:

```bash
mvn clean test
```

## License

MIT License. See [LICENSE](LICENSE) for details.
