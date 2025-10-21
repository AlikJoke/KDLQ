package ru.joke.kdlq.internal.consumers;

import ru.joke.kdlq.*;
import ru.joke.kdlq.internal.configs.KDLQConfigurationRegistry;
import ru.joke.kdlq.internal.routers.KDLQMessageRouter;
import ru.joke.kdlq.internal.routers.KDLQMessageRouterFactory;
import ru.joke.kdlq.internal.util.Args;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.function.Consumer;

/**
 * Default thread-safe implementation of the consumer factory {@link KDLQMessageConsumerFactory}.
 *
 * @author Alik
 * @see KDLQMessageConsumer
 */
@ThreadSafe
public final class ConfigurableKDLQMessageConsumerFactory implements KDLQMessageConsumerFactory {

    private final KDLQConfigurationRegistry configsRegistry;
    private final KDLQMessageRouterFactory messageRouterFactory;
    private final Consumer<KDLQMessageConsumer<?, ?>> onCloseCallback;

    /**
     * Constructs the factory.
     *
     * @param registry             configuration registry; cannot be {@code null}.
     * @param messageRouterFactory message router factory; cannot be {@code null}.
     * @param onCloseCallback      callback that should be called when the consumer is closed; cannot be {@code null}.
     */
    public ConfigurableKDLQMessageConsumerFactory(
            @Nonnull final KDLQConfigurationRegistry registry,
            @Nonnull final KDLQMessageRouterFactory messageRouterFactory,
            @Nonnull final Consumer<KDLQMessageConsumer<?, ?>> onCloseCallback
    ) {
        this.configsRegistry = Args.requireNotNull(registry, () -> new KDLQException("Registry must be not null"));
        this.messageRouterFactory = Args.requireNotNull(messageRouterFactory, () -> new KDLQException("Router factory must be not null"));
        this.onCloseCallback = Args.requireNotNull(onCloseCallback, () -> new KDLQException("Callback must be not null"));
    }

    @Override
    @Nonnull
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
                router,
                this.onCloseCallback
        );
    }

    @Override
    @Nonnull
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
