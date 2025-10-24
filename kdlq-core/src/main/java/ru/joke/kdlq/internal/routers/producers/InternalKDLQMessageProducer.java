package ru.joke.kdlq.internal.routers.producers;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQException;
import ru.joke.kdlq.internal.util.Args;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.TimeUnit;

@ThreadSafe
final class InternalKDLQMessageProducer<K, V> implements KDLQMessageProducer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(InternalKDLQMessageProducer.class);

    private static final int WAIT_TIMEOUT = 30;

    private final KDLQProducersRegistry producersRegistry;
    private final KDLQConfiguration configuration;

    InternalKDLQMessageProducer(
            @Nonnull final KDLQProducersRegistry producersRegistry,
            @Nonnull final KDLQConfiguration configuration
    ) {
        this.producersRegistry = Args.requireNotNull(producersRegistry, () -> new KDLQException("Producers registry must be not null"));
        this.configuration = Args.requireNotNull(configuration, () -> new KDLQException("Configuration must be not null"));
    }

    @Override
    public void send(@Nonnull ProducerRecord<K, V> record) throws Exception {
        final var producerSession = createProducerSession();
        producerSession.producer().send(record, (recordMetadata, e) -> {
            if (e != null) {
                throw new KDLQException(e);
            }
        }).get(WAIT_TIMEOUT, TimeUnit.SECONDS);
    }

    private KDLQProducerSession<K, V> createProducerSession() {
        final KDLQProducerSession<K, V> session = this.producersRegistry.registerIfNeed(
                this.configuration.producerId(),
                () -> new InternalKDLQProducerSession<>(this.configuration)
        );

        if (!session.onUsage()) {
            return createProducerSession();
        }

        return session;
    }

    @Override
    public void close() {
        final var producerSession = this.producersRegistry.get(this.configuration.producerId());
        producerSession
                .stream()
                .peek(session -> logger.info("Closing KDLQ message producer"))
                .filter(session -> session.close(WAIT_TIMEOUT, () -> this.producersRegistry.unregister(session.sessionId())))
                .findAny()
                .ifPresent(session -> logger.info("KDLQ message producer closed"));
    }
}
