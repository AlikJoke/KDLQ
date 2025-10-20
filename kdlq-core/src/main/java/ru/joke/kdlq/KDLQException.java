package ru.joke.kdlq;

/**
 * Base exception for all types of exceptions that can be thrown by KDLQ.
 *
 * @author Alik
 */
public class KDLQException extends RuntimeException {

    /**
     * Constructs a new exception with {@code null} as its cause and provided detail message.
     *
     * @param message detail message; can be {@code null}.
     */
    public KDLQException(final String message) {
        super(message);
    }

    /**
     * Constructs a new exception with cause and {@code null} as its detail message.
     *
     * @param cause the cause; can be {@code null}.
     */
    public KDLQException(final Exception cause) {
        super(cause);
    }
}
