package ru.joke.kdlq.internal.routers.producers;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Supplier;

public sealed interface KDLQProducersRegistry permits InternalKDLQProducersRegistry {

    @Nonnull
    <K, V> KDLQProducerSession<K, V> registerIfNeed(
            @Nonnull String producerSessionId,
            @Nonnull Supplier<KDLQProducerSession<K, V>> sessionSupplier
    );

    @Nonnull
    <K, V> Optional<KDLQProducerSession<K, V>> get(@Nonnull String producerSessionId);

    void unregister(@Nonnull String producerSessionId);
}
