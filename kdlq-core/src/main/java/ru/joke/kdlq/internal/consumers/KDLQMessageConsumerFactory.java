package ru.joke.kdlq.internal.consumers;

import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQMessageConsumer;
import ru.joke.kdlq.KDLQMessageProcessor;

import javax.annotation.Nonnull;

/**
 * KDLQ consumer ({@link KDLQMessageConsumer}) factory.
 *
 * @author Alik
 * @see KDLQMessageConsumer
 */
public sealed interface KDLQMessageConsumerFactory permits ConfigurableKDLQMessageConsumerFactory {

    /**
     * Creates message consumer with specified parameters.
     *
     * @param id               consumer id; cannot be {@code null} or empty.
     * @param dlqConfiguration consumer configuration; cannot be {@code null}.
     * @param messageProcessor message processor; cannot be {@code null}.
     * @return created consumer; cannot be {@code null}.
     * @param <K> type of message key
     * @param <V> type of message body
     * @see KDLQConfiguration
     * @see KDLQMessageProcessor
     * @see KDLQMessageConsumer
     */
    @Nonnull
    <K, V> KDLQMessageConsumer<K, V> create(
            @Nonnull String id,
            @Nonnull KDLQConfiguration dlqConfiguration,
            @Nonnull KDLQMessageProcessor<K, V> messageProcessor
    );

    /**
     * Creates message consumer with specified parameters.
     *
     * @param id               consumer id; cannot be {@code null} or empty.
     * @param configurationId  consumer configuration id; cannot be {@code null}.
     * @param messageProcessor message processor; cannot be {@code null}.
     * @return created consumer; cannot be {@code null}.
     * @param <K> type of message key
     * @param <V> type of message body
     * @see KDLQConfiguration
     * @see KDLQMessageProcessor
     * @see KDLQMessageConsumer
     */
    @Nonnull
    <K, V> KDLQMessageConsumer<K, V> create(
            @Nonnull String id,
            @Nonnull String configurationId,
            @Nonnull KDLQMessageProcessor<K, V> messageProcessor
    );
}
