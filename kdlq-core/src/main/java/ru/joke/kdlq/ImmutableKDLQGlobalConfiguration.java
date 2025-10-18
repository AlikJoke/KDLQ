package ru.joke.kdlq;

import ru.joke.kdlq.internal.util.Args;
import ru.joke.kdlq.internal.util.DaemonThreadFactory;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public record ImmutableKDLQGlobalConfiguration(
        ScheduledExecutorService redeliveryPool,
        long redeliveryTaskDelay,
        KDLQGlobalDistributedLockService distributedLockService,
        KDLQRedeliveryStorage redeliveryStorage
) implements KDLQGlobalConfiguration {

    public ImmutableKDLQGlobalConfiguration {
        Args.requireNotNull(redeliveryPool, () -> new KDLQConfigurationException("Redelivery pool must be not null"));
        Args.requireNotNull(distributedLockService, () -> new KDLQConfigurationException("Distributed lock service must be not null"));
        Args.requireNotNull(redeliveryStorage, () -> new KDLQConfigurationException("Redelivery storage must be not null"));

        if (redeliveryTaskDelay <= 0) {
            throw new KDLQConfigurationException("Redelivery delay must be positive");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private ScheduledExecutorService redeliveryPool;
        private long redeliveryTaskDelay = 1_000;
        private KDLQGlobalDistributedLockService distributedLockService;
        private KDLQRedeliveryStorage redeliveryStorage;

        public Builder withRedeliveryPool(ScheduledExecutorService redeliveryPool) {
            this.redeliveryPool = redeliveryPool;
            return this;
        }

        public Builder withRedeliveryTaskDelay(long redeliveryTaskDelay) {
            this.redeliveryTaskDelay = redeliveryTaskDelay;
            return this;
        }

        public Builder withDistributedLockService(KDLQGlobalDistributedLockService distributedLockService) {
            this.distributedLockService = distributedLockService;
            return this;
        }

        public Builder withRedeliveryStorage(KDLQRedeliveryStorage redeliveryStorage) {
            this.redeliveryStorage = redeliveryStorage;
            return this;
        }

        public KDLQGlobalConfiguration build() {
            final var redeliveryPool =
                    this.redeliveryPool == null
                            ? Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("redelivery"))
                            : this.redeliveryPool;
            return new ImmutableKDLQGlobalConfiguration(
                    redeliveryPool,
                    this.redeliveryTaskDelay,
                    this.distributedLockService,
                    this.redeliveryStorage
            );
        }
    }
}
