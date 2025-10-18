package ru.joke.kdlq.internal.routers;

import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.function.Supplier;

@ThreadSafe
public final class DefaultKDLQMessageRouterFactory implements KDLQMessageRouterFactory {

    private final KDLQMessageSenderFactory messageSenderFactory;
    private final Supplier<KDLQRedeliveryStorage> redeliveryStorageFactory;

    public DefaultKDLQMessageRouterFactory(
            @Nonnull KDLQMessageSenderFactory messageSenderFactory,
            @Nonnull Supplier<KDLQRedeliveryStorage> redeliveryStorageFactory
    ) {
        this.messageSenderFactory = messageSenderFactory;
        this.redeliveryStorageFactory = redeliveryStorageFactory;
    }

    @Nonnull
    @Override
    public <K, V> KDLQMessageRouter<K, V> create(
            @Nonnull String id,
            @Nonnull KDLQConfiguration configuration
    ) {
        final KDLQMessageSender<K, V> sender = this.messageSenderFactory.create(configuration);
        return new DefaultKDLQMessageRouter<>(
                id,
                configuration,
                sender,
                this.redeliveryStorageFactory
        );
    }
}
