package ru.joke.kdlq.internal.configs;

import ru.joke.kdlq.ImmutableKDLQGlobalConfiguration;
import ru.joke.kdlq.KDLQGlobalConfiguration;
import ru.joke.kdlq.KDLQGlobalConfigurationFactory;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import java.util.concurrent.ScheduledExecutorService;

public class DefaultKDLQGlobalConfigurationFactory implements KDLQGlobalConfigurationFactory {

    @Override
    public KDLQGlobalConfiguration createStatelessStandaloneConfiguration(
            ScheduledExecutorService redeliveryPool,
            long redeliveryTaskDelay
    ) {
        return createStandaloneConfiguration(
                redeliveryPool,
                redeliveryTaskDelay,
                new InMemoryKDLQRedeliveryStorage()
        );
    }

    @Override
    public KDLQGlobalConfiguration createStatelessStandaloneConfiguration(long redeliveryTaskDelay) {
        return createStatelessStandaloneConfiguration(
                null,
                redeliveryTaskDelay
        );
    }

    @Override
    public KDLQGlobalConfiguration createStandaloneConfiguration(
            ScheduledExecutorService redeliveryPool,
            long redeliveryTaskDelay,
            KDLQRedeliveryStorage redeliveryStorage
    ) {
        return createConfiguration(
                redeliveryPool,
                redeliveryTaskDelay,
                new StandaloneKDLQGlobalDistributedLockService(),
                redeliveryStorage
        );
    }

    @Override
    public KDLQGlobalConfiguration createStandaloneConfiguration(
            long redeliveryTaskDelay,
            KDLQRedeliveryStorage redeliveryStorage
    ) {
        return createStandaloneConfiguration(
                null,
                redeliveryTaskDelay,
                redeliveryStorage
        );
    }

    @Override
    public KDLQGlobalConfiguration createConfiguration(
            ScheduledExecutorService redeliveryPool,
            long redeliveryTaskDelay,
            KDLQGlobalDistributedLockService lockService,
            KDLQRedeliveryStorage redeliveryStorage
    ) {
        final var builder = ImmutableKDLQGlobalConfiguration.builder();

        return builder
                    .withRedeliveryPool(redeliveryPool)
                    .withRedeliveryStorage(redeliveryStorage)
                    .withRedeliveryTaskDelay(redeliveryTaskDelay)
                    .withDistributedLockService(lockService)
                .build();
    }

    @Override
    public KDLQGlobalConfiguration createConfiguration(
            long redeliveryTaskDelay,
            KDLQGlobalDistributedLockService lockService,
            KDLQRedeliveryStorage redeliveryStorage
    ) {
        final var builder = ImmutableKDLQGlobalConfiguration.builder();

        return builder
                    .withRedeliveryStorage(redeliveryStorage)
                    .withRedeliveryTaskDelay(redeliveryTaskDelay)
                    .withDistributedLockService(lockService)
                .build();
    }

}
