package ru.joke.kdlq.internal.configs;

import ru.joke.kdlq.KDLQConfiguration;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of the KDLQ configs ({@link KDLQConfiguration}) registry
 * based on the {@link ConcurrentHashMap}.
 *
 * @author Alik
 * @see KDLQConfigurationRegistry
 */
@ThreadSafe
public final class DefaultKDLQConfigurationRegistry implements KDLQConfigurationRegistry {

    private final Map<String, KDLQConfiguration> registry = new ConcurrentHashMap<>();

    @Override
    public boolean register(@Nonnull KDLQConfiguration configuration) {
        return this.registry.putIfAbsent(configuration.id(), configuration) == null;
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
