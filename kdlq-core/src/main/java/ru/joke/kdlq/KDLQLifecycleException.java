package ru.joke.kdlq;

/**
 * Exception generated when the order of the method's calls of the KDLQ components is violated.
 *
 * @author Alik
 * @see KDLQException
 */
public class KDLQLifecycleException extends KDLQException {

    /**
     * Constructs a new exception with {@code null} as its cause and provided detail message.
     *
     * @param message detail message; can be {@code null}.
     */
    public KDLQLifecycleException(final String message) {
        super(message);
    }
}
