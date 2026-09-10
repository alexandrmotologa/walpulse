package com.engine.walpulse.domain.model;

import java.util.Objects;

/**
 * Represents a PostgreSQL 64-bit Log Sequence Number (LSN).
 * Pure Java domain value object without external dependencies.
 */
public final class LsnPosition implements Comparable<LsnPosition> {

    public static final LsnPosition ZERO = new LsnPosition(0L);

    private final long value;

    public LsnPosition(long value) {
        this.value = value;
    }

    /**
     * Parses an LSN string formatted as "X/Y" or "X/YYYYYYYY" (hexadecimal).
     *
     * @param lsnStr string representation of LSN, e.g. "0/16B4F80"
     * @return parsed LsnPosition
     */
    public static LsnPosition valueOf(String lsnStr) {
        if (lsnStr == null || lsnStr.isBlank()) {
            return ZERO;
        }

        int slashIndex = lsnStr.indexOf('/');
        if (slashIndex <= 0) {
            try {
                return new LsnPosition(Long.parseUnsignedLong(lsnStr.trim(), 16));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid LSN format: " + lsnStr, e);
            }
        }

        long high = Long.parseUnsignedLong(lsnStr.substring(0, slashIndex).trim(), 16);
        long low = Long.parseUnsignedLong(lsnStr.substring(slashIndex + 1).trim(), 16);
        long combined = (high << 32) | (low & 0xFFFFFFFFL);
        return new LsnPosition(combined);
    }

    public static LsnPosition fromLong(long lsnLong) {
        return new LsnPosition(lsnLong);
    }

    public long asLong() {
        return value;
    }

    /**
     * Formats LSN into the standard PostgreSQL format: "HIGH/LOW".
     */
    public String asString() {
        long high = (value >>> 32) & 0xFFFFFFFFL;
        long low = value & 0xFFFFFFFFL;
        return Long.toHexString(high).toUpperCase() + "/" + Long.toHexString(low).toUpperCase();
    }

    /**
     * Calculates the distance in bytes between this LSN and an earlier LSN.
     */
    public long distanceTo(LsnPosition other) {
        if (other == null) {
            return value;
        }
        return Math.abs(this.value - other.value);
    }

    @Override
    public int compareTo(LsnPosition other) {
        return Long.compareUnsigned(this.value, other.value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LsnPosition that = (LsnPosition) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return asString();
    }
}
