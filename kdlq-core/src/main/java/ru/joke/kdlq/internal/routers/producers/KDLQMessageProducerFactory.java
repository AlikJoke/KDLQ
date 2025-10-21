package ru.joke.kdlq.internal.routers.producers;

import ru.joke.kdlq.KDLQConfiguration;

import javax.annotation.Nonnull;

public sealed interface KDLQMessageProducerFactory permits InternalKDLQMessageProducerFactory {

    <K, V> KDLQMessageProducer<K, V> create(@Nonnull KDLQConfiguration configuration);
}
