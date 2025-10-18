package ru.joke.kdlq.internal.configs;

import ru.joke.kdlq.KDLQConfiguration;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class DefaultKDLQConfigurationRegistry implements KDLQConfigurationRegistry {

    private final Map<String, KDLQConfiguration> registry = new ConcurrentHashMap<>();

    @Override
    public boolean register(@Nonnull KDLQConfiguration configuration) {
        return this.registry.putIfAbsent(configuration.id(), configuration) == null;
    }

    @Override
    public boolean unregister(@Nonnull KDLQConfiguration configuration) {
        return this.registry.remove(configuration.id(), configuration);
    }

    @Override
    public boolean unregister(@Nonnull String configurationId) {
        return this.registry.remove(configurationId) != null;
    }

    @Override
    @Nonnull
    public Optional<KDLQConfiguration> get(@Nonnull String configurationId) {
        return Optional.of(this.registry.get(configurationId));
    }

    @Override
    @Nonnull
    public Set<KDLQConfiguration> getAll() {
        return new HashSet<>(this.registry.values());
    }
}
