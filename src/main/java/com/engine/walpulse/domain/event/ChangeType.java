package com.engine.walpulse.domain.event;

/**
 * Supported row-level mutation types captured from Write-Ahead Logs.
 */
public enum ChangeType {
    INSERT("I"),
    UPDATE("U"),
    DELETE("D"),
    TRUNCATE("T");

    private final String code;

    ChangeType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static ChangeType fromCode(char code) {
        return switch (code) {
            case 'I', 'i' -> INSERT;
            case 'U', 'u' -> UPDATE;
            case 'D', 'd' -> DELETE;
            case 'T', 't' -> TRUNCATE;
            default -> throw new IllegalArgumentException("Unknown change type code: " + code);
        };
    }
}
