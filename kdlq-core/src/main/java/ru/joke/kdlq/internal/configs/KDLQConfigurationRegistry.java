package ru.joke.kdlq.internal.configs;

import ru.joke.kdlq.KDLQConfiguration;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

/**
 * Registry of the KDLQ configuration objects {@link KDLQConfiguration}.
 *
 * @author Alik
 * @see KDLQConfiguration
 */
public sealed interface KDLQConfigurationRegistry permits InternalKDLQConfigurationRegistry {

    /**
     * Registers the provided configuration object if a configuration with the same identifier
     * is not already registered.
     *
     * @param configuration configuration to register; cannot be {@code null}.
     * @return {@code true} if the configuration was registered successfully;
     *         {@code false} if a configuration with that identifier was already registered.
     */
    boolean register(@Nonnull KDLQConfiguration configuration);

    /**
     * Unregisters the provided configuration object if a configuration with the
     * same identifier is registered.
     *
     * @param configuration configuration to unregister; cannot be {@code null}.
     * @return {@code true} if the configuration was unregistered successfully;
     *         {@code false} if a configuration with that identifier was not registered previously.
     */
    default boolean unregister(@Nonnull KDLQConfiguration configuration) {
        return unregister(configuration.id());
    }

    /**
     * Unregisters the configuration with provided id if a configuration with the
     * same identifier is registered.
     *
     * @param configurationId configuration id to unregister; cannot be {@code null} or empty.
     * @return {@code true} if the configuration was unregistered successfully;
     *         {@code false} if a configuration with that identifier was not registered previously.
     */
    boolean unregister(@Nonnull String configurationId);

    /**
     * Returns the configuration with provided id if a configuration with the same identifier is registered.
     * @param configurationId configuration id; cannot be {@code null} or empty.
     * @return {@link Optional#empty()} if a configuration with that identifier was not registered previously;
     *         not {@code null} object wrapped in {@link Optional} otherwise.
     */
    @Nonnull
    Optional<KDLQConfiguration> get(@Nonnull String configurationId);

    /**
     * Returns all registered configurations.
     *
     * @return all registered configurations; cannot be {@code null}.
     */
    @Nonnull
    Set<KDLQConfiguration> getAll();
}
