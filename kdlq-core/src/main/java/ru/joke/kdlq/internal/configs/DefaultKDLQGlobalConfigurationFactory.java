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
import java.util.concurrent.ExecutorService;
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
            @Nullable final ScheduledExecutorService redeliveryDispatcherPool,
            @Nullable final ExecutorService redeliveryPool,
            @Nonnegative long redeliveryDispatcherTaskDelay
    ) {
        return createStandaloneConfiguration(
                redeliveryDispatcherPool,
                redeliveryPool,
                redeliveryDispatcherTaskDelay,
                new InMemoryKDLQRedeliveryStorage()
        );
    }

    @Override
    @Nonnull
    public KDLQGlobalConfiguration createStatelessStandaloneConfiguration(@Nonnegative long redeliveryDispatcherTaskDelay) {
        return createStatelessStandaloneConfiguration(
                null,
                null,
                redeliveryDispatcherTaskDelay
        );
    }

    @Override
    @Nonnull
    public KDLQGlobalConfiguration createStandaloneConfiguration(
            @Nullable final ScheduledExecutorService redeliveryDispatcherPool,
            @Nullable final ExecutorService redeliveryPool,
            @Nonnegative final long redeliveryDispatcherTaskDelay,
            @Nonnull final KDLQRedeliveryStorage redeliveryStorage
    ) {
        return createCustomConfiguration(
                redeliveryDispatcherPool,
                redeliveryPool,
                redeliveryDispatcherTaskDelay,
                new StandaloneKDLQGlobalDistributedLockService(),
                redeliveryStorage
        );
    }

    @Override
    @Nonnull
    public KDLQGlobalConfiguration createStandaloneConfiguration(
            @Nonnegative final long redeliveryDispatcherTaskDelay,
            @Nonnull final KDLQRedeliveryStorage redeliveryStorage
    ) {
        return createStandaloneConfiguration(
                null,
                null,
                redeliveryDispatcherTaskDelay,
                redeliveryStorage
        );
    }

    @Override
    @Nonnull
    public KDLQGlobalConfiguration createCustomConfiguration(
            @Nullable final ScheduledExecutorService redeliveryDispatcherPool,
            @Nullable final ExecutorService redeliveryPool,
            @Nonnegative final long redeliveryDispatcherTaskDelay,
            @Nonnull final KDLQGlobalDistributedLockService lockService,
            @Nonnull final KDLQRedeliveryStorage redeliveryStorage
    ) {
        final var builder = ImmutableKDLQGlobalConfiguration.builder();

        return builder
                    .withRedeliveryDispatcherPool(redeliveryDispatcherPool)
                    .withRedeliveryDispatcherTaskDelay(redeliveryDispatcherTaskDelay)
                    .withRedeliveryPool(redeliveryPool)
                .build(lockService, redeliveryStorage);
    }

    @Override
    @Nonnull
    public KDLQGlobalConfiguration createCustomConfiguration(
            long redeliveryDispatcherTaskDelay,
            @Nonnull KDLQGlobalDistributedLockService lockService,
            @Nonnull KDLQRedeliveryStorage redeliveryStorage
    ) {
        final var builder = ImmutableKDLQGlobalConfiguration.builder();

        return builder
                    .withRedeliveryDispatcherTaskDelay(redeliveryDispatcherTaskDelay)
                .build(lockService, redeliveryStorage);
    }

}
