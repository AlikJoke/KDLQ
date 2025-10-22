package ru.joke.kdlq.internal.routers.producers;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Registry of the KDLQ producer sessions.
 *
 * @author Alik
 * @see KDLQProducerSession
 */
public sealed interface KDLQProducersRegistry permits InternalKDLQProducersRegistry {

    /**
     * Registers a session with the specified id if a session with that
     * id is not already registered.
     *
     * @param producerSessionId session id; cannot be {@code null} or empty.
     * @param sessionSupplier   session supplier; cannot be {@code null}.
     * @return created or already existed session; cannot be {@code null}.
     * @param <K> type of message key
     * @param <V> type of message body
     */
    @Nonnull
    <K, V> KDLQProducerSession<K, V> registerIfNeed(
            @Nonnull String producerSessionId,
            @Nonnull Supplier<KDLQProducerSession<K, V>> sessionSupplier
    );

    /**
     * Returns the session by its id.
     *
     * @param producerSessionId session id; cannot be {@code null} or empty.
     * @return session in {@link Optional} or {@link Optional#empty()} if
     *         session with such id is not registered.
     * @param <K> type of message key
     * @param <V> type of message body
     */
    @Nonnull
    <K, V> Optional<KDLQProducerSession<K, V>> get(@Nonnull String producerSessionId);

    /**
     * Unregisters session with specified id.
     *
     * @param producerSessionId session id to unregister; cannot be {@code null} or empty.
     */
    void unregister(@Nonnull String producerSessionId);
}
