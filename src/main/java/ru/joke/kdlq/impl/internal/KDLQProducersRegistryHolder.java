package ru.joke.kdlq.impl.internal;

import javax.annotation.Nonnull;

final class KDLQProducersRegistryHolder {

    private static final KDLQProducersRegistry registry = new KDLQProducersRegistry();

    @Nonnull
    static KDLQProducersRegistry get() {
        return registry;
    }
}
