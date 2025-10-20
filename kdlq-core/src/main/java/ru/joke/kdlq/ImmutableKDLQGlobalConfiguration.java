package ru.joke.kdlq;

import ru.joke.kdlq.internal.util.Args;
import ru.joke.kdlq.internal.util.DaemonThreadFactory;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Immutable implementation of the KDLQ global configuration {@link KDLQGlobalConfiguration}.<br>
 * For more convenient construction of the configuration object, use the builder {@link #builder()}.
 *
 * @param redeliveryPool redelivery pool; cannot be {@code null}.
 * @param redeliveryTaskDelay redelivery task delay; cannot be {@code <= 0}.
 * @param distributedLockService distributed locking service; cannot be {@code null}.
 * @param redeliveryStorage redelivery storage; cannot be {@code null}.
 *
 * @author Alik
 * @see KDLQGlobalConfiguration
 * @see KDLQGlobalConfigurationFactory
 * @see Builder
 * @see #builder()
 */
@ThreadSafe
@Immutable
public record ImmutableKDLQGlobalConfiguration(
        @Nonnull ScheduledExecutorService redeliveryPool,
        @Nonnegative long redeliveryTaskDelay,
        @Nonnull KDLQGlobalDistributedLockService distributedLockService,
        @Nonnull KDLQRedeliveryStorage redeliveryStorage
) implements KDLQGlobalConfiguration {

    /**
     * Constructs configuration object with specified parameters.
     *
     * @param redeliveryPool redelivery pool; cannot be {@code null}.
     * @param redeliveryTaskDelay redelivery task delay; cannot be {@code <= 0}.
     * @param distributedLockService distributed locking service; cannot be {@code null}.
     * @param redeliveryStorage redelivery storage; cannot be {@code null}.
     */
    public ImmutableKDLQGlobalConfiguration {
        Args.requireNotNull(redeliveryPool, () -> new KDLQConfigurationException("Redelivery pool must be not null"));
        Args.requireNotNull(distributedLockService, () -> new KDLQConfigurationException("Distributed lock service must be not null"));
        Args.requireNotNull(redeliveryStorage, () -> new KDLQConfigurationException("Redelivery storage must be not null"));

        if (redeliveryTaskDelay <= 0) {
            throw new KDLQConfigurationException("Redelivery delay must be positive");
        }
    }

    /**
     * Returns a builder instance for more convenient construction of the configuration object.
     *
     * @return builder instance; cannot be {@code null}.
     */
    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for the KDLQ global configuration object.<br>
     * This class is not thread-safe.
     */
    @NotThreadSafe
    public static final class Builder {

        private ScheduledExecutorService redeliveryPool;
        private long redeliveryTaskDelay = 1_000;
        private KDLQGlobalDistributedLockService distributedLockService;
        private KDLQRedeliveryStorage redeliveryStorage;

        /**
         * Sets the thread pool for running the message redelivery task.
         *
         * @param redeliveryPool redelivery pool; if pool is not provided, a single-threaded
         *                       pool managed by KDLQ will be created.
         * @return the current builder object for further construction; cannot be {@code null}.
         * @see KDLQGlobalConfiguration#redeliveryPool()
         */
        @Nonnull
        public Builder withRedeliveryPool(final ScheduledExecutorService redeliveryPool) {
            this.redeliveryPool = redeliveryPool;
            return this;
        }

        /**
         * Sets the delay in milliseconds for the message redelivery task.
         *
         * @param redeliveryTaskDelay task delay in milliseconds; if delay is not provided,
         *                            a default value will be applied (1s).
         * @return the current builder object for further construction; cannot be {@code null}.
         * @see KDLQGlobalConfiguration#redeliveryTaskDelay()
         */
        @Nonnull
        public Builder withRedeliveryTaskDelay(final long redeliveryTaskDelay) {
            this.redeliveryTaskDelay = redeliveryTaskDelay;
            return this;
        }

        /**
         * Creates a global configuration object with the specified parameters.<br>
         * Mandatory parameters are passed as arguments to this method; others are optional,
         * and if not explicitly set in the builder, default values will be used.
         *
         * @param distributedLockService distributed locking service; cannot be {@code null}.
         * @param redeliveryStorage      redelivery storage; cannot be {@code null}.
         * @return created configuration object; cannot be {@code null}.
         * @see KDLQGlobalConfiguration#distributedLockService()
         * @see KDLQGlobalConfiguration#redeliveryStorage()
         */
        @Nonnull
        public KDLQGlobalConfiguration build(
                @Nonnull final KDLQGlobalDistributedLockService distributedLockService,
                @Nonnull final KDLQRedeliveryStorage redeliveryStorage
        ) {
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
