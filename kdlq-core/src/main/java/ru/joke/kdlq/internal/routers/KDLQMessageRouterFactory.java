package ru.joke.kdlq.internal.routers;

import ru.joke.kdlq.KDLQConfiguration;

import javax.annotation.Nonnull;

public interface KDLQMessageRouterFactory {

    <K, V> KDLQMessageRouter<K, V> create(
            @Nonnull String id,
            @Nonnull KDLQConfiguration configuration
    );
}
