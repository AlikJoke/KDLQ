package ru.joke.kdlq.internal.consumers;

import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQMessageConsumer;
import ru.joke.kdlq.KDLQMessageProcessor;

import javax.annotation.Nonnull;

public interface KDLQMessageConsumerFactory {

    <K, V> KDLQMessageConsumer<K, V> create(
            @Nonnull String id,
            @Nonnull KDLQConfiguration dlqConfiguration,
            @Nonnull KDLQMessageProcessor<K, V> messageProcessor
    );

    <K, V> KDLQMessageConsumer<K, V> create(
            @Nonnull String id,
            @Nonnull String configurationId,
            @Nonnull KDLQMessageProcessor<K, V> messageProcessor
    );
}
