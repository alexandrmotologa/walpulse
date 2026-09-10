package com.engine.walpulse.domain.event;

/**
 * State machine representing the lifecycle of the CDC streaming engine.
 */
public enum ReplicationState {
    IDLE,
    STARTING,
    STREAMING,
    STOPPING,
    STOPPED,
    FAILED
}
