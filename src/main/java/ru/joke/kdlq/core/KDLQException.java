package ru.joke.kdlq.core;

import javax.annotation.Nonnull;

public final class KDLQException extends RuntimeException {

    public KDLQException(@Nonnull Exception ex) {
        super(ex);
    }
}
