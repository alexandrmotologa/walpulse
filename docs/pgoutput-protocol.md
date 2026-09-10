# PostgreSQL pgoutput replication protocol reference

PostgreSQL logical decoding transmits raw WAL changes formatted by output plugins. The default built-in plugin since PostgreSQL 10 is `pgoutput`. WalPulse connects using `pgoutput` to avoid requiring third-party C extensions like `wal2json` or `decoderbufs`.

## Message types and frame structures

Each frame begins with a single ASCII byte identifying the message type:

| Type Byte | Message Name | Description |
|---|---|---|
| `'B'` (0x42) | Begin | Marks the start of a transaction. Contains final LSN, commit timestamp, and 32-bit transaction ID (XID). |
| `'C'` (0x43) | Commit | Marks transaction commit. Contains flags, commit LSN, transaction end LSN, and commit timestamp. |
| `'R'` (0x52) | Relation | Schema definition for a table. Emitted once per transaction before changes to that table, or whenever table schema changes. Contains relation OID, namespace, table name, replica identity, column count, and per-column types. |
| `'Y'` (0x59) | Type | Emitted for composite or custom data types. Contains type OID, namespace, and name. |
| `'I'` (0x49) | Insert | Emitted on row insertion. Contains relation OID, tuple flag `'N'` (new tuple), column count, and column byte streams. |
| `'U'` (0x55) | Update | Emitted on row update. Contains relation OID, optional old key/tuple flag (`'K'` for replica identity key, `'O'` for old tuple), and new tuple flag `'N'`. |
| `'D'` (0x44) | Delete | Emitted on row deletion. Contains relation OID, and old tuple/key flag (`'K'` or `'O'`). |
| `'T'` (0x54) | Truncate | Emitted on table truncation. Contains cascade flags and list of relation OIDs truncated. |

## Tuple data encoding

Column values within `'I'`, `'U'`, and `'D'` messages follow a compact column encoding format:

- `'n'` (0x6E): Null column value. No data length or bytes follow.
- `'u'` (0x75): Unchanged TOASTed value. Indicates large out-of-line data has not changed in an update.
- `'t'` (0x74): Text-formatted column value. Followed by a 4-byte big-endian integer specifying text byte length, then the raw UTF-8 string bytes.
- `'b'` (0x62): Binary-formatted column value (used when binary mode is enabled).

## Schema caching and dynamic synchronization

Because row tuples in `'I'`, `'U'`, and `'D'` frames contain column values in ordinal sequence without repeating column names, WalPulse maintains a `SchemaCache` keyed by relation OID.

Whenever an `'R'` message arrives, WalPulse parses column names, data type OIDs, and key flags, updating the in-memory schema definition. Subsequent data messages look up column names from the cache to construct structured JSON payloads.
