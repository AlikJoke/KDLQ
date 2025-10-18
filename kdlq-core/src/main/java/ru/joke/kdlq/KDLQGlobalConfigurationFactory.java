package ru.joke.kdlq;

import ru.joke.kdlq.internal.configs.DefaultKDLQGlobalConfigurationFactory;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import java.util.concurrent.ScheduledExecutorService;

public interface KDLQGlobalConfigurationFactory {

    KDLQGlobalConfiguration createStatelessStandaloneConfiguration(
            ScheduledExecutorService redeliveryPool,
            long redeliveryTaskDelay
    );

    KDLQGlobalConfiguration createStatelessStandaloneConfiguration(long redeliveryTaskDelay);

    KDLQGlobalConfiguration createStandaloneConfiguration(
            ScheduledExecutorService redeliveryPool,
            long redeliveryTaskDelay,
            KDLQRedeliveryStorage redeliveryStorage
    );

    KDLQGlobalConfiguration createStandaloneConfiguration(
            long redeliveryTaskDelay,
            KDLQRedeliveryStorage redeliveryStorage
    );

    KDLQGlobalConfiguration createConfiguration(
            ScheduledExecutorService redeliveryPool,
            long redeliveryTaskDelay,
            KDLQGlobalDistributedLockService lockService,
            KDLQRedeliveryStorage redeliveryStorage
    );

    KDLQGlobalConfiguration createConfiguration(
            long redeliveryTaskDelay,
            KDLQGlobalDistributedLockService lockService,
            KDLQRedeliveryStorage redeliveryStorage
    );

    static KDLQGlobalConfigurationFactory getInstance() {
        return new DefaultKDLQGlobalConfigurationFactory();
    }
}
