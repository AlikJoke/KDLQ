package ru.joke.kdlq.internal.configs;

import ru.joke.kdlq.KDLQConfiguration;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

public interface KDLQConfigurationRegistry {

    boolean register(@Nonnull KDLQConfiguration configuration);

    boolean unregister(@Nonnull KDLQConfiguration configuration);

    boolean unregister(@Nonnull String configurationId);

    @Nonnull
    Optional<KDLQConfiguration> get(@Nonnull String configurationId);

    @Nonnull
    Set<KDLQConfiguration> getAll();
}
