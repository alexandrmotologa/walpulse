package com.engine.walpulse.domain.exception;

/**
 * Thrown when creating, dropping, or querying a PostgreSQL logical replication slot fails.
 */
public class ReplicationSlotException extends WalStreamException {

    private final String slotName;

    public ReplicationSlotException(String slotName, String message) {
        super("Replication slot error ['" + slotName + "']: " + message);
        this.slotName = slotName;
    }

    public ReplicationSlotException(String slotName, String message, Throwable cause) {
        super("Replication slot error ['" + slotName + "']: " + message, cause);
        this.slotName = slotName;
    }

    public String getSlotName() {
        return slotName;
    }
}
