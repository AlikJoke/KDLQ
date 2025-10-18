package ru.joke.kdlq;

import javax.annotation.Nonnull;

/**
 * Exception that occurs when the KDLQ configuration is configured incorrectly.
 *
 * @author Alik
 * @see KDLQException
 */
public final class KDLQConfigurationException extends KDLQException {

    public KDLQConfigurationException(@Nonnull String message) {
        super(message);
    }
}
