package ru.joke.kdlq;

import javax.annotation.Nonnull;

public final class KDLQMessageMustBeKilledException extends KDLQException {

    public KDLQMessageMustBeKilledException(@Nonnull Exception cause) {
        super(cause);
    }

    public KDLQMessageMustBeKilledException(@Nonnull String message) {
        super(message);
    }
}
