package ru.joke.kdlq;

import javax.annotation.Nonnull;

/**
 * Base exception for all types of exceptions that can be thrown by KDLQ.
 *
 * @author Alik
 */
public class KDLQException extends RuntimeException {

    public KDLQException(@Nonnull String message) {
        super(message);
    }

    public KDLQException(@Nonnull Exception cause) {
        super(cause);
    }
}
