package com.engine.walpulse.domain.model;

/**
 * Represents a single column value from a replication tuple.
 *
 * @param name       column name
 * @param typeOid    PostgreSQL data type OID
 * @param typeName   inferred or mapped type name (e.g. text, int4, jsonb)
 * @param value      string or parsed representation of the value (null if isNull is true)
 * @param isKey      whether column is part of the replica identity or primary key
 * @param isNull     true if column value is explicit SQL NULL
 * @param isToast    true if column is an unchanged TOAST pointer
 */
public record ColumnValue(
        String name,
        int typeOid,
        String typeName,
        Object value,
        boolean isKey,
        boolean isNull,
        boolean isToast
) {
    public static ColumnValue of(String name, int typeOid, String typeName, Object value, boolean isKey) {
        return new ColumnValue(name, typeOid, typeName, value, isKey, value == null, false);
    }

    public static ColumnValue nullValue(String name, int typeOid, String typeName, boolean isKey) {
        return new ColumnValue(name, typeOid, typeName, null, isKey, true, false);
    }

    public static ColumnValue unchangedToast(String name, int typeOid, String typeName, boolean isKey) {
        return new ColumnValue(name, typeOid, typeName, null, isKey, false, true);
    }
}
