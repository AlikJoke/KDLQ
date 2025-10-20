package ru.joke.kdlq;

/**
 * Exception generated when the actions with configuration object is not allowed in current state.
 *
 * @author Alik
 * @see KDLQLifecycleException
 */
public final class KDLQConfigurationLifecycleException extends KDLQLifecycleException {

    /**
     * Constructs a new exception with {@code null} as its cause and provided detail message.
     *
     * @param message detail message; can be {@code null}.
     */
    public KDLQConfigurationLifecycleException(String message) {
        super(message);
    }
}
