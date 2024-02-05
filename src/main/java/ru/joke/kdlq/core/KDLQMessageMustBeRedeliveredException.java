package ru.joke.kdlq.core;

import javax.annotation.Nonnull;

public final class KDLQMessageMustBeRedeliveredException extends KDLQException {

    public KDLQMessageMustBeRedeliveredException(@Nonnull Exception cause) {
        super(cause);
    }
}
