package ru.joke.kdlq;

import ru.joke.kdlq.internal.configs.DefaultKDLQGlobalConfigurationFactory;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A factory for KDLQ global configuration instances, providing a more convenient way
 * to construct a configuration object based on various configuration parameters.<br>
 * An alternative to using this factory is the standard implementation
 * with a builder via {@link ImmutableKDLQGlobalConfiguration}.
 *
 * @author Alik
 * @see KDLQGlobalConfiguration
 * @see KDLQGlobalConfigurationFactory#getInstance()
 * @see ImmutableKDLQGlobalConfiguration#builder()
 */
public interface KDLQGlobalConfigurationFactory {

    /**
     * Creates a configuration object for a standalone application with an in-memory
     * (stateless) storage, using the specified thread pool to run the message
     * redelivery task at the specified frequency.<br>
     * The library does not manage the lifecycle of the thread pool if it is created outside the KDLQ.<br><br>
     * <b>Data will be lost when using this configuration, as it is stored in memory.
     * Furthermore, using this type of data storage can lead to {@link OutOfMemoryError} errors (OOM).<br>
     * Not suitable for use in a distributed or production system!</b>
     *
     * @param redeliveryPool      specified thread pool to run redelivery task; can be {@code null}
     *                            (in such case a single-threaded pool managed by the KDLQ library will be created).
     * @param redeliveryTaskDelay specified redelivery task frequency as millis; cannot be {@code <= 0}.
     * @return global configuration object with provided parameters; cannot be {@code null}.
     */
    @Nonnull
    KDLQGlobalConfiguration createStatelessStandaloneConfiguration(
            @Nullable ScheduledExecutorService redeliveryPool,
            @Nonnegative long redeliveryTaskDelay
    );

    /**
     * Creates a configuration object for a standalone application with an in-memory
     * (stateless) storage on a single-threaded pool managed by the KDLQ library,
     * to run the message redelivery task at the specified frequency.<br><br>
     * <b>Data will be lost when using this configuration, as it is stored in memory.
     * Furthermore, using this type of data storage can lead to {@link OutOfMemoryError} errors (OOM).<br>
     * Not suitable for use in a distributed or production system!</b>
     *
     * @param redeliveryTaskDelay specified redelivery task frequency as millis; cannot be {@code <= 0}.
     * @return global configuration object with provided parameters; cannot be {@code null}.
     */
    @Nonnull
    KDLQGlobalConfiguration createStatelessStandaloneConfiguration(@Nonnegative long redeliveryTaskDelay);

    /**
     * Creates a configuration object for a standalone application with the specified
     * storage and the specified thread pool to run the message redelivery task at
     * the specified frequency.<br>
     * The library does not manage the lifecycle of the thread pool if it is created outside the KDLQ.<br>
     * <b>Not suitable for use in a distributed system!</b>
     *
     * @param redeliveryPool      specified thread pool to run redelivery task; can be {@code null}
     *                            (in such case a single-threaded pool managed by the KDLQ library will be created).
     * @param redeliveryTaskDelay specified redelivery task frequency as millis; cannot be {@code <= 0}.
     * @param redeliveryStorage   specified redelivery storage; cannot be {@code null}.
     * @return global configuration object with provided parameters; cannot be {@code null}.
     */
    @Nonnull
    KDLQGlobalConfiguration createStandaloneConfiguration(
            @Nullable ScheduledExecutorService redeliveryPool,
            @Nonnegative long redeliveryTaskDelay,
            @Nonnull KDLQRedeliveryStorage redeliveryStorage
    );

    /**
     * Creates a configuration object for a standalone application with the specified
     * storage and the specified message redelivery task frequency. A single-threaded
     * pool to run the redelivery task will be created and managed by the library.<br>
     *
     * <b>Not suitable for use in a distributed system!</b>
     *
     * @param redeliveryTaskDelay specified redelivery task frequency as millis; cannot be {@code <= 0}.
     * @param redeliveryStorage   specified redelivery storage; cannot be {@code null}.
     * @return global configuration object with provided parameters; cannot be {@code null}.
     */
    @Nonnull
    KDLQGlobalConfiguration createStandaloneConfiguration(
            @Nonnegative long redeliveryTaskDelay,
            @Nonnull KDLQRedeliveryStorage redeliveryStorage
    );

    /**
     * Creates a configuration object for an application with the specified storage,
     * distributed locking service, and thread pool to run the message redelivery task
     * at the specified frequency.<br>
     * The library does not manage the lifecycle of the thread pool if it is created outside the KDLQ.<br>
     * <b>The implementations of the storage and distributed locking service must meet
     * the requirements specified in the documentation for these services for use in a
     * distributed application.</b>
     *
     * @param redeliveryPool      specified thread pool to run redelivery task; can be {@code null}
     *                            (in such case a single-threaded pool managed by the KDLQ library will be created).
     * @param redeliveryTaskDelay specified redelivery task frequency as millis; cannot be {@code <= 0}.
     * @param redeliveryStorage   specified redelivery storage; cannot be {@code null}.
     * @param lockService         specified distributed locking service; cannot be {@code null}.
     * @return global configuration object with provided parameters; cannot be {@code null}.
     */
    @Nonnull
    KDLQGlobalConfiguration createCustomConfiguration(
            @Nullable ScheduledExecutorService redeliveryPool,
            @Nonnegative long redeliveryTaskDelay,
            @Nonnull KDLQGlobalDistributedLockService lockService,
            @Nonnull KDLQRedeliveryStorage redeliveryStorage
    );

    /**
     * Creates a configuration object for an application with the specified storage,
     * distributed locking service and specified message redelivery task
     * frequency. A single-threaded pool to run the redelivery task will be created
     * and managed by the library.<br>
     * <b>The implementations of the storage and distributed locking service must meet
     * the requirements specified in the documentation for these services for use in a
     * distributed application.</b>
     *
     * @param redeliveryTaskDelay specified redelivery task frequency as millis; cannot be {@code <= 0}.
     * @param redeliveryStorage   specified redelivery storage; cannot be {@code null}.
     * @param lockService         specified distributed locking service; cannot be {@code null}.
     * @return global configuration object with provided parameters; cannot be {@code null}.
     */
    @Nonnull
    KDLQGlobalConfiguration createCustomConfiguration(
            @Nonnegative long redeliveryTaskDelay,
            @Nonnull KDLQGlobalDistributedLockService lockService,
            @Nonnull KDLQRedeliveryStorage redeliveryStorage
    );

    /**
     * Returns the default implementation of this factory.
     *
     * @return implementation of this factory; cannot be {@code null}.
     */
    @Nonnull
    static KDLQGlobalConfigurationFactory getInstance() {
        return new DefaultKDLQGlobalConfigurationFactory();
    }
}
