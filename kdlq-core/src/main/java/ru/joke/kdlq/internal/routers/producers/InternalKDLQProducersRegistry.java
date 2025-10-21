package ru.joke.kdlq.internal.routers.producers;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public final class InternalKDLQProducersRegistry implements KDLQProducersRegistry {

    private final Map<String, KDLQProducerSession<?, ?>> producersById = new ConcurrentHashMap<>();

    @Override
    @Nonnull
    public <K, V> KDLQProducerSession<K, V> registerIfNeed(@Nonnull String producerSessionId, @Nonnull Supplier<KDLQProducerSession<K, V>> sessionSupplier) {
        @SuppressWarnings("unchecked")
        final var result = (KDLQProducerSession<K, V>) this.producersById.computeIfAbsent(producerSessionId, k -> sessionSupplier.get());
        return result;
    }

    @Override
    @Nonnull
    public <K, V> Optional<KDLQProducerSession<K, V>> get(@Nonnull String producerSessionId) {
        @SuppressWarnings("unchecked")
        final var result = (KDLQProducerSession<K, V>) this.producersById.get(producerSessionId);
        return Optional.ofNullable(result);
    }

    @Override
    public void unregister(@Nonnull String producerSessionId) {
        this.producersById.remove(producerSessionId);
    }
}
