package ru.joke.kdlq.core;

import javax.annotation.Nonnull;

public final class KDLQLifecycleException extends KDLQException {

    public KDLQLifecycleException(@Nonnull String message) {
        super(message);
    }
}
