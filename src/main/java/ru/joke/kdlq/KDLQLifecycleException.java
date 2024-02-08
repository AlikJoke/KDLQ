package ru.joke.kdlq;

import javax.annotation.Nonnull;

/**
 * Exception generated when the order of the method's calls of the KDLQ components is violated.
 *
 * @author Alik
 * @see KDLQException
 */
public final class KDLQLifecycleException extends KDLQException {

    public KDLQLifecycleException(@Nonnull String message) {
        super(message);
    }
}
