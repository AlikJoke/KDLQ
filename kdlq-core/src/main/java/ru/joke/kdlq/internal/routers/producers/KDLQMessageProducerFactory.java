package ru.joke.kdlq.internal.routers.producers;

import ru.joke.kdlq.KDLQConfiguration;

import javax.annotation.Nonnull;

/**
 * A factory of the KDLQ message producers {@link KDLQMessageProducer}.
 *
 * @author Alik
 * @see KDLQMessageProducer
 */
public sealed interface KDLQMessageProducerFactory permits InternalKDLQMessageProducerFactory {

    /**
     * Creates producer with provided configuration.
     *
     * @param configuration provided configuration; cannot be {@code null}.
     * @return created producer; cannot be {@code null}.
     * @param <K> type of message key
     * @param <V> type of message body
     */
    @Nonnull
    <K, V> KDLQMessageProducer<K, V> create(@Nonnull KDLQConfiguration configuration);
}
