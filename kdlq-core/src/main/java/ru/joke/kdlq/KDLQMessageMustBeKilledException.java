package ru.joke.kdlq;

/**
 * The exception thrown if the message must be killed and routed to DLQ.
 *
 * @author Alik
 * @see KDLQException
 * @see KDLQMessageMustBeRedeliveredException
 */
public final class KDLQMessageMustBeKilledException extends KDLQException {

    /**
     * Constructs a new exception with cause and {@code null} as its detail message.
     *
     * @param cause the cause; can be {@code null}.
     */
    public KDLQMessageMustBeKilledException(final Exception cause) {
        super(cause);
    }

    /**
     * Constructs a new exception with {@code null} as its cause and provided detail message.
     *
     * @param message detail message; can be {@code null}.
     */
    public KDLQMessageMustBeKilledException(final String message) {
        super(message);
    }
}
