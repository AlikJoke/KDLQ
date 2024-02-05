package ru.joke.kdlq.core;

import javax.annotation.Nonnull;

public class KDLQException extends RuntimeException {

    public KDLQException(@Nonnull String message) {
        super(message);
    }

    public KDLQException(@Nonnull Exception cause) {
        super(cause);
    }
}
