package ru.joke.kdlq.internal.routers.producers;

import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQException;
import ru.joke.kdlq.internal.util.Args;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of the message producer factory.
 *
 * @author Alik
 * @see KDLQMessageProducer
 * @see KDLQMessageProducerFactory
 */
@ThreadSafe
public final class InternalKDLQMessageProducerFactory implements KDLQMessageProducerFactory {

    private final KDLQProducersRegistry producersRegistry;

    /**
     * Constructs factory.
     *
     * @param producersRegistry registry of KDLQ producers; cannot be {@code null}.
     */
    public InternalKDLQMessageProducerFactory(@Nonnull KDLQProducersRegistry producersRegistry) {
        this.producersRegistry = Args.requireNotNull(producersRegistry, () -> new KDLQException("Registry must be not null"));
    }

    @Override
    @Nonnull
    public <K, V> KDLQMessageProducer<K, V> create(@Nonnull KDLQConfiguration configuration) {
        return new InternalKDLQMessageProducer<>(this.producersRegistry, configuration);
    }
}
