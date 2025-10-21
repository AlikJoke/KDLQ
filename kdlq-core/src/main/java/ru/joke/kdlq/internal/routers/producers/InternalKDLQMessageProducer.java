package ru.joke.kdlq.internal.routers.producers;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQException;

import java.util.concurrent.TimeUnit;

final class InternalKDLQMessageProducer<K, V> implements KDLQMessageProducer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(InternalKDLQMessageProducer.class);

    private static final int WAIT_TIMEOUT = 30;

    private final KDLQProducersRegistry producersRegistry;
    private final KDLQConfiguration configuration;

    InternalKDLQMessageProducer(
            final KDLQProducersRegistry producersRegistry,
            final KDLQConfiguration configuration
    ) {
        this.producersRegistry = producersRegistry;
        this.configuration = configuration;
    }

    @Override
    public void send(ProducerRecord<K, V> record) throws Exception {
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
