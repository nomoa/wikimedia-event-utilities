package org.wikimedia.eventutilities.core;

import java.io.Serializable;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * A clock that is serializable.
 */
@FunctionalInterface
public interface SerializableClock extends Supplier<Instant>, Serializable {
    /**
     * A clock that is frozen in time.
     */
    static SerializableClock frozenClock(Instant instant) {
        return () -> instant;
    }
}
