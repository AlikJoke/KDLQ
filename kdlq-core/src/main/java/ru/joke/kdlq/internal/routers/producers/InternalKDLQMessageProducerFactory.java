package ru.joke.kdlq.internal.routers.producers;

import ru.joke.kdlq.KDLQConfiguration;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class InternalKDLQMessageProducerFactory implements KDLQMessageProducerFactory {

    private final KDLQProducersRegistry producersRegistry;

    public InternalKDLQMessageProducerFactory(@Nonnull KDLQProducersRegistry producersRegistry) {
        this.producersRegistry = producersRegistry;
    }

    @Nonnull
    public <K, V> KDLQMessageProducer<K, V> create(@Nonnull KDLQConfiguration configuration) {
        return new InternalKDLQMessageProducer<>(this.producersRegistry, configuration);
    }
}
