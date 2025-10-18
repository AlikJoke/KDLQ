package ru.joke.kdlq;

import ru.joke.kdlq.internal.configs.DefaultKDLQConfigurationRegistry;
import ru.joke.kdlq.internal.configs.KDLQConfigurationRegistry;
import ru.joke.kdlq.internal.consumers.DefaultKDLQMessageConsumerFactory;
import ru.joke.kdlq.internal.consumers.KDLQMessageConsumerFactory;
import ru.joke.kdlq.internal.redelivery.RedeliveryTask;
import ru.joke.kdlq.internal.routers.DefaultKDLQMessageRouterFactory;
import ru.joke.kdlq.internal.routers.DefaultKDLQMessageSenderFactory;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnull;

public final class KDLQ {

    private static final KDLQ instance = new KDLQ();

    private final KDLQConfigurationRegistry configurationRegistry;
    private final KDLQMessageConsumerFactory messageConsumerFactory;
    private final RedeliveryStorageHolder redeliveryStorageHolder;
    private final DefaultKDLQMessageSenderFactory messageSenderFactory;
    private volatile boolean initialized;

    private KDLQ() {
        this.messageSenderFactory = new DefaultKDLQMessageSenderFactory();
        this.redeliveryStorageHolder = new RedeliveryStorageHolder();
        this.configurationRegistry = new DefaultKDLQConfigurationRegistry();
        final var routerFactory = new DefaultKDLQMessageRouterFactory(
                this.messageSenderFactory,
                () -> this.redeliveryStorageHolder.storage
        );
        this.messageConsumerFactory = new DefaultKDLQMessageConsumerFactory(
                this.configurationRegistry,
                routerFactory
        );
    }

    public static synchronized void initialize(final KDLQGlobalConfiguration globalConfiguration) {
        if (instance.initialized) {
            throw new KDLQLifecycleException("KDLQ already initialized");
        }

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
    }

    public static void destroy() {
        // TODO
    }

    public static <K, V> KDLQMessageConsumer<K, V> createConsumer(
            @Nonnull String id,
            @Nonnull KDLQConfiguration dlqConfiguration,
            @Nonnull KDLQMessageProcessor<K, V> messageProcessor
    ) {
        return instance.messageConsumerFactory.create(
                id,
                dlqConfiguration,
                messageProcessor
        );
    }

    public static <K, V> KDLQMessageConsumer<K, V> createConsumer(
            @Nonnull String id,
            @Nonnull String configurationId,
            @Nonnull KDLQMessageProcessor<K, V> messageProcessor
    ) {
        final var dlqConfiguration =
                instance.configurationRegistry.get(configurationId).orElseThrow(() -> new KDLQException("KDLQ configuration not found by provided id: " + configurationId));

        return createConsumer(id, dlqConfiguration, messageProcessor);
    }

    public static boolean registerConfiguration(KDLQConfiguration configuration) {
        return instance.configurationRegistry.register(configuration);
    }

    public static boolean unregisterConfiguration(KDLQConfiguration configuration) {
        return instance.configurationRegistry.unregister(configuration);
    }

    public static boolean unregisterConfiguration(String configurationId) {
        return instance.configurationRegistry.unregister(configurationId);
    }

    private static class RedeliveryStorageHolder {
        private volatile KDLQRedeliveryStorage storage;
    }
}
