package ru.joke.kdlq.internal.consumers;

import ru.joke.kdlq.*;
import ru.joke.kdlq.internal.configs.KDLQConfigurationRegistry;
import ru.joke.kdlq.internal.routers.KDLQMessageRouter;
import ru.joke.kdlq.internal.routers.KDLQMessageRouterFactory;

import javax.annotation.Nonnull;

public final class DefaultKDLQMessageConsumerFactory implements KDLQMessageConsumerFactory {

    private final KDLQConfigurationRegistry configsRegistry;
    private final KDLQMessageRouterFactory messageRouterFactory;

    public DefaultKDLQMessageConsumerFactory(
            final KDLQConfigurationRegistry registry,
            final KDLQMessageRouterFactory messageRouterFactory
    ) {
        this.configsRegistry = registry;
        this.messageRouterFactory = messageRouterFactory;
    }

    @Override
    public <K, V> KDLQMessageConsumer<K, V> create(
            @Nonnull String id,
            @Nonnull KDLQConfiguration dlqConfiguration,
            @Nonnull KDLQMessageProcessor<K, V> messageProcessor
    ) {
        final KDLQMessageRouter<K, V> router = this.messageRouterFactory.create(id, dlqConfiguration);
        return new ConfigurableKDLQMessageConsumer<>(
                id,
                dlqConfiguration,
                messageProcessor,
                router
        );
    }

    @Override
    public <K, V> KDLQMessageConsumer<K, V> create(
            @Nonnull String id,
            @Nonnull String dlqConfigurationId,
            @Nonnull KDLQMessageProcessor<K, V> messageProcessor
    ) {
        final var dlqConfiguration =
                this.configsRegistry.get(dlqConfigurationId).orElseThrow(() -> new KDLQException("KDLQ configuration not found by provided id: " + dlqConfigurationId));

        return create(
                id,
                dlqConfiguration,
                messageProcessor
        );
    }
}
