package ru.joke.kdlq;

/**
 * The exception thrown if the message must be redelivered for the next processing attempt.
 *
 * @author Alik
 * @see KDLQException
 * @see KDLQMessageMustBeKilledException
 */
public final class KDLQMessageMustBeRedeliveredException extends KDLQException {

    /**
     * Constructs a new exception with cause and {@code null} as its detail message.
     *
     * @param cause the cause; can be {@code null}.
     */
    public KDLQMessageMustBeRedeliveredException(final Exception cause) {
        super(cause);
    }

    /**
     * Constructs a new exception with {@code null} as its cause and provided detail message.
     *
     * @param message detail message; can be {@code null}.
     */
    public KDLQMessageMustBeRedeliveredException(final String message) {
        super(message);
    }
}
