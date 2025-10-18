package ru.joke.kdlq.internal.routers;

import javax.annotation.Nonnull;

final class KDLQProducersRegistryHolder {

    private static final KDLQProducersRegistry registry = new KDLQProducersRegistry();

    @Nonnull
    static KDLQProducersRegistry get() {
        return registry;
    }
}
