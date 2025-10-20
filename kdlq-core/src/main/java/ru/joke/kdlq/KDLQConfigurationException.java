package ru.joke.kdlq;

/**
 * Exception that occurs when the KDLQ configuration is configured incorrectly.
 *
 * @author Alik
 * @see KDLQException
 */
public final class KDLQConfigurationException extends KDLQException {

    /**
     * Constructs a new exception with {@code null} as its cause and provided detail message.
     *
     * @param message detail message; can be {@code null}.
     */
    public KDLQConfigurationException(final String message) {
        super(message);
    }
}
