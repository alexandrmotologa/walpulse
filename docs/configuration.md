# WalPulse configuration guide

WalPulse configuration options can be defined in `src/main/resources/application.yml` or overridden via environment variables or command-line parameters.

## Configuration reference

```yaml
walpulse:
  # PostgreSQL Connection Settings
  postgres:
    host: ${WALPULSE_PG_HOST:localhost}
    port: ${WALPULSE_PG_PORT:5432}
    database: ${WALPULSE_PG_DB:postgres}
    username: ${WALPULSE_PG_USER:postgres}
    password: ${WALPULSE_PG_PASSWORD:postgres}
    slot-name: ${WALPULSE_SLOT_NAME:walpulse_slot}
    publication-name: ${WALPULSE_PUB_NAME:walpulse_pub}
    create-slot-if-missing: ${WALPULSE_CREATE_SLOT:true}
    status-interval-ms: ${WALPULSE_STATUS_INTERVAL_MS:5000}

  # Filtering and PII Masking
  filter:
    include-tables:
      - "public.*"
      - "ecommerce.*"
    exclude-tables:
      - "public.schema_migrations"
      - "public.flyway_schema_history"
    masked-fields:
      - "password"
      - "password_hash"
      - "token"
      - "secret"
      - "credit_card"
      - "ssn"

  # Sink Selection and Parameters
  sink:
    type: ${WALPULSE_SINK_TYPE:logging} # Options: logging, kafka, webhook, composite

    kafka:
      bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
      topic-prefix: ${KAFKA_TOPIC_PREFIX:cdc.}
      acks: ${KAFKA_ACKS:all}
      retries: ${KAFKA_RETRIES:3}

    webhook:
      url: ${WEBHOOK_URL:http://localhost:9000/webhook}
      secret: ${WEBHOOK_SECRET:secret-token}
      timeout-ms: ${WEBHOOK_TIMEOUT_MS:3000}
      max-retries: ${WEBHOOK_MAX_RETRIES:3}

server:
  port: 8080

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
```

## Environment variable overrides

Every YAML parameter maps to an uppercase environment variable:

- `WALPULSE_PG_HOST`: PostgreSQL server host.
- `WALPULSE_PG_PORT`: PostgreSQL port.
- `WALPULSE_PG_DB`: PostgreSQL database name.
- `WALPULSE_PG_USER`: Replication user.
- `WALPULSE_PG_PASSWORD`: Password.
- `WALPULSE_SLOT_NAME`: Replication slot identifier.
- `WALPULSE_PUB_NAME`: Publication name.
- `WALPULSE_SINK_TYPE`: Chosen sink type (`logging`, `kafka`, `webhook`, or `composite`).
