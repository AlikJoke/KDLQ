package ru.joke.kdlq.internal.configs;

import ru.joke.kdlq.ImmutableKDLQGlobalConfiguration;
import ru.joke.kdlq.KDLQGlobalConfiguration;
import ru.joke.kdlq.KDLQGlobalConfigurationFactory;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Default implementation of the global configuration ({@link KDLQGlobalConfiguration}) factory {@link KDLQGlobalConfigurationFactory}.
 *
 * @author Alik
 */
@ThreadSafe
@Immutable
public final class DefaultKDLQGlobalConfigurationFactory implements KDLQGlobalConfigurationFactory {

    @Override
    @Nonnull
    public KDLQGlobalConfiguration createStatelessStandaloneConfiguration(
            @Nullable final ScheduledExecutorService redeliveryPool,
            @Nonnegative long redeliveryTaskDelay
    ) {
        return createStandaloneConfiguration(
                redeliveryPool,
                redeliveryTaskDelay,
                new InMemoryKDLQRedeliveryStorage()
        );
    }

    @Override
    @Nonnull
    public KDLQGlobalConfiguration createStatelessStandaloneConfiguration(@Nonnegative long redeliveryTaskDelay) {
        return createStatelessStandaloneConfiguration(
                null,
                redeliveryTaskDelay
        );
    }

    @Override
    @Nonnull
    public KDLQGlobalConfiguration createStandaloneConfiguration(
            @Nullable final ScheduledExecutorService redeliveryPool,
            @Nonnegative final long redeliveryTaskDelay,
            @Nonnull final KDLQRedeliveryStorage redeliveryStorage
    ) {
        return createCustomConfiguration(
                redeliveryPool,
                redeliveryTaskDelay,
                new StandaloneKDLQGlobalDistributedLockService(),
                redeliveryStorage
        );
    }

    @Override
    @Nonnull
    public KDLQGlobalConfiguration createStandaloneConfiguration(
            @Nonnegative final long redeliveryTaskDelay,
            @Nonnull final KDLQRedeliveryStorage redeliveryStorage
    ) {
        return createStandaloneConfiguration(
                null,
                redeliveryTaskDelay,
                redeliveryStorage
        );
    }

    @Override
    @Nonnull
    public KDLQGlobalConfiguration createCustomConfiguration(
            @Nullable final ScheduledExecutorService redeliveryPool,
            @Nonnegative final long redeliveryTaskDelay,
            @Nonnull final KDLQGlobalDistributedLockService lockService,
            @Nonnull final KDLQRedeliveryStorage redeliveryStorage
    ) {
        final var builder = ImmutableKDLQGlobalConfiguration.builder();

        return builder
                    .withRedeliveryPool(redeliveryPool)
                    .withRedeliveryTaskDelay(redeliveryTaskDelay)
                .build(lockService, redeliveryStorage);
    }

    @Override
    @Nonnull
    public KDLQGlobalConfiguration createCustomConfiguration(
            long redeliveryTaskDelay,
            @Nonnull KDLQGlobalDistributedLockService lockService,
            @Nonnull KDLQRedeliveryStorage redeliveryStorage
    ) {
        final var builder = ImmutableKDLQGlobalConfiguration.builder();

        return builder
                    .withRedeliveryTaskDelay(redeliveryTaskDelay)
                .build(lockService, redeliveryStorage);
    }

}
