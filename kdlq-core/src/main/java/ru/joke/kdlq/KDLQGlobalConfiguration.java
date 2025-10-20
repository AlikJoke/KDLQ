package ru.joke.kdlq;

import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Global KDLQ configuration, aggregating settings common to all KDLQ consumers.
 *
 * @author Alik
 * @see KDLQGlobalDistributedLockService
 * @see KDLQRedeliveryStorage
 */
public sealed interface KDLQGlobalConfiguration permits ImmutableKDLQGlobalConfiguration {

    /**
     * Returns the thread pool that should be used for redelivering messages if delayed
     * redelivery settings are present. The library will only use a single thread from
     * this pool for redelivery, therefore a single-threaded pool is sufficient.<br>
     * If the thread pool instance is provided externally rather than being created by
     * the KDLQ library, the responsibility for closing this pool lies with the client
     * module using KDLQ.
     *
     * @return redelivery thread pool; cannot be {@code null}.
     */
    @Nonnull
    ScheduledExecutorService redeliveryPool();

    /**
     * Returns the frequency, in milliseconds, at which the message redelivery task should operate.
     *
     * @return redelivery task frequency; cannot be {@code <= 0}.
     */
    @Nonnegative
    long redeliveryTaskDelay();

    /**
     * Returns the distributed locking service used by the message redelivery task to
     * ensure that only one KDLQ instance performs message redelivery.<br>
     * In a distributed system without this service, messages may be redelivered multiple times.<br>
     * In the case of a standalone system, the locking service can be local
     * (e.g., based on {@link java.util.concurrent.locks.Lock}).
     *
     * @return distributed locking service; cannot be {@code null}.
     * @see KDLQGlobalDistributedLockService
     */
    @Nonnull
    KDLQGlobalDistributedLockService distributedLockService();

    /**
     * Returns the storage for messages that are to be redelivered with a delay.<br>
     * For reliable operation, this storage must be persistent and distributed
     * (i.e., different KDLQ instances in a distributed system must have access to this storage instance).
     *
     * @return redelivery storage; cannot be {@code null}.
     * @see KDLQRedeliveryStorage
     */
    @Nonnull
    KDLQRedeliveryStorage redeliveryStorage();

    /**
     * Returns a builder instance for more convenient construction of the global configuration object.
     *
     * @return builder instance; cannot be {@code null}.
     * @see ImmutableKDLQGlobalConfiguration.Builder
     */
    @Nonnull
    static ImmutableKDLQGlobalConfiguration.Builder builder() {
        return new ImmutableKDLQGlobalConfiguration.Builder();
    }
}
