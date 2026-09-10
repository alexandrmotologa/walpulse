package com.engine.walpulse.domain.exception;

/**
 * Base exception thrown during WAL stream decoding, connection, or transport failures.
 */
public class WalStreamException extends RuntimeException {

    public WalStreamException(String message) {
        super(message);
    }

    public WalStreamException(String message, Throwable cause) {
        super(message, cause);
    }
}
