package ru.joke.kdlq.internal.routers;

import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.internal.routers.headers.KDLQHeadersService;
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducer;
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducerFactory;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.function.Supplier;

@ThreadSafe
public final class InternalKDLQMessageRouterFactory implements KDLQMessageRouterFactory {

    private final KDLQMessageProducerFactory messageSenderFactory;
    private final Supplier<KDLQRedeliveryStorage> redeliveryStorageFactory;
    private final KDLQHeadersService headersService;

    public InternalKDLQMessageRouterFactory(
            @Nonnull KDLQMessageProducerFactory messageSenderFactory,
            @Nonnull Supplier<KDLQRedeliveryStorage> redeliveryStorageFactory,
            @Nonnull KDLQHeadersService headersService
    ) {
        this.messageSenderFactory = messageSenderFactory;
        this.redeliveryStorageFactory = redeliveryStorageFactory;
        this.headersService = headersService;
    }

    @Nonnull
    @Override
    public <K, V> KDLQMessageRouter<K, V> create(
            @Nonnull String id,
            @Nonnull KDLQConfiguration configuration
    ) {
        final KDLQMessageProducer<K, V> sender = this.messageSenderFactory.create(configuration);
        return new InternalKDLQMessageRouter<>(
                id,
                configuration,
                sender,
                this.redeliveryStorageFactory,
                this.headersService
        );
    }
}
