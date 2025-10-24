package ru.joke.kdlq.internal.routers;

import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQException;
import ru.joke.kdlq.internal.routers.headers.KDLQHeadersService;
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducer;
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducerFactory;
import ru.joke.kdlq.internal.util.Args;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.function.Supplier;

/**
 * Implementation of the message router factory.
 *
 * @author Alik
 * @see KDLQMessageProducerFactory
 * @see KDLQMessageRouter
 */
@ThreadSafe
@Immutable
public final class InternalKDLQMessageRouterFactory implements KDLQMessageRouterFactory {

    private final KDLQMessageProducerFactory messageSenderFactory;
    private final Supplier<KDLQRedeliveryStorage> redeliveryStorageFactory;
    private final KDLQHeadersService headersService;

    /**
     * Constructs router factory.
     *
     * @param messageSenderFactory     message producers factory; cannot be {@code null}.
     * @param redeliveryStorageFactory redelivery storage factory; cannot be {@code null}.
     * @param headersService           KDLQ headers service; cannot be {@code null}.
     */
    public InternalKDLQMessageRouterFactory(
            @Nonnull KDLQMessageProducerFactory messageSenderFactory,
            @Nonnull Supplier<KDLQRedeliveryStorage> redeliveryStorageFactory,
            @Nonnull KDLQHeadersService headersService
    ) {
        this.messageSenderFactory = Args.requireNotNull(messageSenderFactory, () -> new KDLQException("Message sender factory must be not null"));
        this.redeliveryStorageFactory = Args.requireNotNull(redeliveryStorageFactory, () -> new KDLQException("Redelivery storage factory must be not null"));
        this.headersService = Args.requireNotNull(headersService, () -> new KDLQException("Headers service must be not null"));
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
