package ru.joke.kdlq;

import javax.annotation.Nonnull;

/**
 * The exception thrown if the message must be redelivered for the next processing attempt.
 *
 * @author Alik
 * @see KDLQException
 */
public final class KDLQMessageMustBeRedeliveredException extends KDLQException {

    public KDLQMessageMustBeRedeliveredException(@Nonnull Exception cause) {
        super(cause);
    }
}
