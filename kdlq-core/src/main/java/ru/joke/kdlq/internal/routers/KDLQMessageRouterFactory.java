package ru.joke.kdlq.internal.routers;

import ru.joke.kdlq.KDLQConfiguration;

import javax.annotation.Nonnull;

/**
 * A factory for message routers that direct messages to redelivery or
 * the DLQ based on KDLQ configuration.
 *
 * @author Alik
 * @see KDLQMessageRouter
 */
public sealed interface KDLQMessageRouterFactory permits InternalKDLQMessageRouterFactory {

    /**
     * Creates message router with specified id and configuration.
     *
     * @param id            router id; cannot be {@code null} or empty.
     * @param configuration provided configuration; cannot be {@code null}.
     * @return created router; cannot be {@code null}.
     * @param <K> type of message key
     * @param <V> type of message body
     */
    @Nonnull
    <K, V> KDLQMessageRouter<K, V> create(
            @Nonnull String id,
            @Nonnull KDLQConfiguration configuration
    );
}
