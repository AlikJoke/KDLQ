package ru.joke.kdlq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.internal.configs.DefaultKDLQConfigurationRegistry;
import ru.joke.kdlq.internal.configs.KDLQConfigurationRegistry;
import ru.joke.kdlq.internal.consumers.DefaultKDLQMessageConsumerFactory;
import ru.joke.kdlq.internal.consumers.KDLQMessageConsumerFactory;
import ru.joke.kdlq.internal.redelivery.RedeliveryTask;
import ru.joke.kdlq.internal.routers.DefaultKDLQMessageRouterFactory;
import ru.joke.kdlq.internal.routers.DefaultKDLQMessageSenderFactory;
import ru.joke.kdlq.internal.routers.KDLQProducersRegistry;
import ru.joke.kdlq.internal.util.Args;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The entry point to the KDLQ library.<br>
 * Before use, the initialization method ({@link KDLQ#initialize()} or {@link KDLQ#initialize(KDLQGlobalConfiguration)})
 * must be called. When the application shuts down, the {@link KDLQ#shutdown()} ()} method must be called.
 *
 * @author Alik
 *
 * @see KDLQ#registerConsumer(String, String, KDLQMessageProcessor)
 * @see KDLQ#registerConsumer(String, KDLQConfiguration, KDLQMessageProcessor)
 */
@ThreadSafe
public final class KDLQ {

    private static final Logger logger = LoggerFactory.getLogger(KDLQ.class);

    private static final KDLQ instance = new KDLQ();

    private final Map<String, Lock> locks;
    private final Map<String, Map.Entry<KDLQConfiguration, KDLQMessageConsumer<?, ?>>> registeredConsumers;
    private final KDLQConfigurationRegistry configurationRegistry;
    private final KDLQMessageConsumerFactory messageConsumerFactory;
    private final RedeliveryStorageHolder redeliveryStorageHolder;
    private final DefaultKDLQMessageSenderFactory messageSenderFactory;
    private volatile boolean initialized;

    private KDLQ() {
        this.locks = new ConcurrentHashMap<>();
        this.registeredConsumers = new ConcurrentHashMap<>();
        final var producersRegistry = new KDLQProducersRegistry();
        this.messageSenderFactory = new DefaultKDLQMessageSenderFactory(producersRegistry);
        this.redeliveryStorageHolder = new RedeliveryStorageHolder();
        this.configurationRegistry = new DefaultKDLQConfigurationRegistry();
        final var routerFactory = new DefaultKDLQMessageRouterFactory(
                this.messageSenderFactory,
                () -> this.redeliveryStorageHolder.storage
        );
        this.messageConsumerFactory = new DefaultKDLQMessageConsumerFactory(
                this.configurationRegistry,
                routerFactory,
                KDLQ::unregisterConsumer
        );
    }

    /**
     * Initializes the KDLQ subsystem with an empty global configuration.<br>
     * Delayed redelivery will not function without a configuration. If delayed redelivery
     * is configured for a particular consumer, an exception will be thrown when
     * attempting to redeliver a message for that consumer. Immediate redelivery,
     * however, will function correctly.
     */
    public static void initialize() {
        initialize(null);
    }

    /**
     * Initializes the KDLQ subsystem with a given global configuration.<br>
     * If a given configuration is not provided then delayed redelivery will not
     * function. If delayed redelivery is configured for a particular consumer,
     * an exception will be thrown when attempting to redeliver a message for that
     * consumer. Immediate redelivery will function correctly.
     *
     * @param globalConfiguration provided global configuration; can be {@code null}.
     * @see KDLQGlobalConfiguration
     * @throws KDLQLifecycleException if the subsystem already initialized
     */
    public static void initialize(@Nullable final KDLQGlobalConfiguration globalConfiguration) {
        if (instance.initialized) {
            throw new KDLQLifecycleException("KDLQ already initialized");
        }

        logger.info("KDLQ initialization started with config: {}", globalConfiguration);

        if (globalConfiguration != null) {
            final var redeliveryTask = new RedeliveryTask(
                    globalConfiguration,
                    instance.configurationRegistry,
                    instance.messageSenderFactory
            );

            redeliveryTask.run();

            instance.redeliveryStorageHolder.storage = globalConfiguration.redeliveryStorage();
        }

        instance.initialized = true;

        logger.info("KDLQ initialized with config: {}", globalConfiguration);
    }

    /**
     * Shutdowns the subsystem. When stopping, all active consumers are closed
     * and unregistered; all registered configurations are unregistered.<br>
     * To resume operation, the subsystem must be activated again through calls
     * to the initialization methods.
     *
     * @throws KDLQLifecycleException if the subsystem is already inactive
     */
    public static synchronized void shutdown() {
        checkInitialized();

        logger.info("KDLQ shutdown started");

        instance.initialized = false;
        for (var consumer : instance.registeredConsumers.values()) {
            closeConsumer(consumer.getValue());
        }

        instance.registeredConsumers.clear();

        instance.redeliveryStorageHolder.storage = null;
        instance.configurationRegistry.getAll().forEach(instance.configurationRegistry::unregister);

        logger.info("KDLQ shutdown completed");
    }

    /**
     * Registers a new KDLQ consumer with the specified id, configuration, and message handler.<br>
     * <b>The KDLQ subsystem must be initialized before calling this method!</b>
     *
     * @param id               id of the consumer (must be unique); cannot be {@code null} or empty.
     * @param dlqConfiguration consumer configuration; cannot be {@code null}.
     * @param messageProcessor message processor; cannot be {@code null}.
     * @return registered consumer with specified parameters; cannot be {@code null}.
     * @param <K> type of key for messages received by the consumer
     * @param <V> type of body for messages received by the consumer
     *
     * @throws KDLQLifecycleException if KDLQ isn't initialized yet
     * @throws KDLQException          if consumer with such id already registered
     */
    @Nonnull
    public static <K, V> KDLQMessageConsumer<K, V> registerConsumer(
            @Nonnull String id,
            @Nonnull KDLQConfiguration dlqConfiguration,
            @Nonnull KDLQMessageProcessor<K, V> messageProcessor
    ) {
        checkInitialized();

        Args.requireNotEmpty(id, () -> new KDLQException("Provided consumer id must be not empty"));
        Args.requireNotNull(dlqConfiguration, () -> new KDLQException("Provided configuration must be not null"));
        Args.requireNotNull(messageProcessor, () -> new KDLQException("Provided processor must be not null"));

        final var lock = findLock(dlqConfiguration.id(), true);
        lock.lock();
        try {
            @SuppressWarnings("unchecked")
            final KDLQMessageConsumer<K, V> result = (KDLQMessageConsumer<K, V>) instance.registeredConsumers.compute(id, (cId, consumer) -> {
                if (consumer != null) {
                    throw new KDLQException("Consumer with such id already registered");
                }

                registerConfiguration(dlqConfiguration);
                final var createdConsumer = instance.messageConsumerFactory.create(id, dlqConfiguration, messageProcessor);

                logger.debug("Consumer {} registered with config {}", id, dlqConfiguration);

                return Map.entry(dlqConfiguration, createdConsumer);
            });

            return result;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Registers a new KDLQ consumer with the specified id, configuration id, and message handler.<br>
     * <b>The KDLQ subsystem must be initialized before calling this method,
     * and a configuration with the provided id must have been registered previously!</b>
     *
     * @param id               id of the consumer (must be unique); cannot be {@code null} or empty.
     * @param configurationId  consumer configuration id; cannot be {@code null} or empty.
     * @param messageProcessor message processor; cannot be {@code null}.
     * @return registered consumer with specified parameters; cannot be {@code null}.
     * @param <K> type of key for messages received by the consumer
     * @param <V> type of body for messages received by the consumer
     *
     * @see KDLQ#registerConfiguration(KDLQConfiguration)
     * @throws KDLQLifecycleException if KDLQ isn't initialized yet
     * @throws KDLQException          if a configuration with this id is not registered yet
     *                                or consumer with such id already registered
     */
    @Nonnull
    public static <K, V> KDLQMessageConsumer<K, V> registerConsumer(
            @Nonnull String id,
            @Nonnull String configurationId,
            @Nonnull KDLQMessageProcessor<K, V> messageProcessor
    ) {
        checkInitialized();

        Args.requireNotEmpty(configurationId, () -> new KDLQException("Provided configuration id must be not empty"));

        final var dlqConfiguration =
                instance.configurationRegistry.get(configurationId).orElseThrow(() -> new KDLQException("KDLQ configuration not found by provided id: " + configurationId));

        return registerConsumer(id, dlqConfiguration, messageProcessor);
    }

    /**
     * Unregisters the given consumer and closes its subscription.
     *
     * @param consumer consumer to unregister; cannot be {@code null}.
     * @param <K> type of key for messages received by the consumer
     * @param <V> type of body for messages received by the consumer
     * @return {@code false} if the consumer is not registered; {@code true} if consumer is closed.
     * @see KDLQMessageConsumer
     * @throws KDLQLifecycleException if KDLQ isn't initialized yet
     */
    public static <K, V> boolean unregisterConsumer(@Nonnull final KDLQMessageConsumer<K, V> consumer) {
        Args.requireNotNull(consumer, () -> new KDLQException("Provided consumer must be not null"));
        return unregisterConsumer(consumer.id());
    }

    /**
     * Unregisters consumer by its id and closes its subscription.
     *
     * @param id consumer id; cannot be {@code null} or empty.
     * @return {@code false} if the consumer is not registered; {@code true} if consumer is closed.
     * @see KDLQMessageConsumer
     * @throws KDLQLifecycleException if KDLQ isn't initialized yet
     */
    public static boolean unregisterConsumer(@Nonnull final String id) {
        checkInitialized();

        Args.requireNotEmpty(id, () -> new KDLQException("Provided consumer id must be not empty"));

        final var consumerEntry = instance.registeredConsumers.get(id);
        if (consumerEntry == null) {
            return false;
        }

        final var lock = findLock(consumerEntry.getKey().id(), true);
        lock.lock();

        try {
            final var consumer = instance.registeredConsumers.remove(id);
            if (consumer == null) {
                return false;
            }

            closeConsumer(consumer.getValue());
        } finally {
            lock.unlock();
        }

        return true;
    }

    /**
     * Registers the given configuration. If the configuration is already registered,
     * the method will not perform any action.
     *
     * @param configuration provided configuration; cannot be {@code null}.
     * @return {@code true} if the configuration is successfully registered;
     * {@code false} if configuration was already registered previously.
     * @see KDLQConfiguration
     * @throws KDLQLifecycleException if KDLQ isn't initialized yet
     */
    public static boolean registerConfiguration(@Nonnull final KDLQConfiguration configuration) {
        return registerConfiguration(configuration, false);
    }

    /**
     * Registers the given configuration. If the configuration is already registered
     * and {@code replaceIfRegistered == true}, the method will replace existing configuration.
     *
     * @param configuration       provided configuration; cannot be {@code null}.
     * @param replaceIfRegistered a flag indicating whether to update the configuration with this id
     *                            if a configuration with the same id is already registered.
     * @return {@code true} if the configuration is successfully registered;
     * {@code false} otherwise.
     * @see KDLQConfiguration
     * @throws KDLQLifecycleException if KDLQ isn't initialized yet
     * @throws KDLQConfigurationLifecycleException if {@code replaceIfRegistered == true} and
     * configuration already registered and some active consumer running with configuration with such id
     */
    public static boolean registerConfiguration(
            @Nonnull final KDLQConfiguration configuration,
            final boolean replaceIfRegistered
    ) {
        checkInitialized();

        Args.requireNotNull(configuration, () -> new KDLQException("Provided configuration must be not null"));

        final var lock = findLock(configuration.id(), true);
        lock.lock();
        try {
            if (replaceIfRegistered) {
                if (instance.registeredConsumers.values()
                        .stream()
                        .anyMatch(c -> c.getKey().id().equals(configuration.id()))) {
                    throw new KDLQConfigurationLifecycleException("There are consumers using this configuration. These consumers must be stopped beforehand.");
                }

                instance.configurationRegistry.unregister(configuration);
            }

            return instance.configurationRegistry.register(configuration);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Unregisters the given configuration. If the configuration is not registered,
     * the method will not perform any action.
     *
     * @param configuration provided configuration; cannot be {@code null}.
     * @return {@code true} if the configuration is successfully unregistered;
     * {@code false} if configuration was not registered previously.
     * @see KDLQConfiguration
     * @throws KDLQLifecycleException if KDLQ isn't initialized yet
     * @throws KDLQConfigurationLifecycleException if some active consumer running with
     * configuration with such id
     */
    public static boolean unregisterConfiguration(@Nonnull final KDLQConfiguration configuration) {
        Args.requireNotNull(configuration, () -> new KDLQException("Provided configuration must be not null"));
        return unregisterConfiguration(configuration.id());
    }

    /**
     * Unregisters the configuration with given id. If the configuration with given id
     * is not registered, the method will not perform any action.
     *
     * @param configurationId provided configuration id; cannot be {@code null}.
     * @return {@code true} if the configuration is successfully unregistered;
     * {@code false} if configuration was not registered previously.
     * @see KDLQConfiguration
     * @throws KDLQLifecycleException if KDLQ isn't initialized yet
     * @throws KDLQConfigurationLifecycleException if some active consumer running with
     * configuration with such id
     */
    public static boolean unregisterConfiguration(@Nonnull final String configurationId) {
        checkInitialized();
        Args.requireNotEmpty(configurationId, () -> new KDLQException("Provided configuration id must be not empty"));

        final var lock = findLock(configurationId, false);
        if (lock == null) {
            return false;
        }

        lock.lock();
        try {
            if (instance.registeredConsumers.values()
                    .stream()
                    .anyMatch(c -> c.getKey().id().equals(configurationId))) {
                throw new KDLQConfigurationLifecycleException("There are consumers using this configuration. These consumers must be stopped beforehand.");
            }

            return instance.configurationRegistry.unregister(configurationId);
        } finally {
            instance.locks.remove(configurationId, lock);
            lock.unlock();
        }
    }

    private static Lock findLock(final String configurationId, final boolean createIfAbsent) {
        return instance.locks.computeIfAbsent(configurationId, k -> createIfAbsent ? new ReentrantLock() : null);
    }

    private static void checkInitialized() {
        if (!instance.initialized) {
            throw new KDLQLifecycleException("KDLQ must be initialized first.");
        }
    }

    private static void closeConsumer(final KDLQMessageConsumer<?, ?> consumer) {
        try {
            consumer.close();
        } catch (RuntimeException ex) {
            logger.warn("Exception when trying to close consumer: " + consumer, ex);
        }
    }

    private static class RedeliveryStorageHolder {
        private volatile KDLQRedeliveryStorage storage;
    }
}
