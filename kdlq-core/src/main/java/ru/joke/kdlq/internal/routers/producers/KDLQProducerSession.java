package ru.joke.kdlq.internal.routers.producers;

import org.apache.kafka.clients.producer.Producer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * Representation of a KDLQ producer session. A session implies the reuse of a single
 * Kafka producer multiple times.
 *
 * @param <K> type of message key
 * @param <V> type of message body
 * @author Alik
 */
public interface KDLQProducerSession<K, V> {

    /**
     * Closes the current session if it is not being used by any other KDLQ component.
     *
     * @param timeoutSeconds  timeout of operation; cannot be negative.
     * @param onCloseCallback callback that should be called when the session is closed;
     *                        cannot be {@code null}.
     * @return {@code true} if the session was closed; {@code false} otherwise.
     */
    boolean close(
            @Nonnegative int timeoutSeconds, 
            @Nonnull Runnable onCloseCallback
    );

    /**
     * Checks if the given session has active consumers.
     *
     * @return {@code true} if session has active consumers; {@code false} otherwise.
     */
    boolean onUsage();

    /**
     * Returns unique session id.
     *
     * @return session id; cannot be {@code null} or empty.
     */
    @Nonnull
    String sessionId();

    /**
     * Returns the Kafka producer used by this session.
     *
     * @return Kafka producer; cannot be {@code null}.
     */
    @Nonnull
    Producer<K, V> producer();
}
