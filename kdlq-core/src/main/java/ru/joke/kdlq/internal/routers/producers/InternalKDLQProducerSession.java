package ru.joke.kdlq.internal.routers.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQLifecycleException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public final class InternalKDLQProducerSession<K, V> implements KDLQProducerSession<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(InternalKDLQProducerSession.class);

    private static final String KDLQ_ID = "KDLQ";

    private final Producer<K, V> producer;
    private final String sessionId;
    private final AtomicInteger usages;
    private volatile boolean isClosed;

    InternalKDLQProducerSession(
            @Nonnull final String sessionId,
            @Nonnull final KafkaProducer<K, V> producer
    ) {
        this.producer = producer;
        this.sessionId = sessionId;
        this.usages = new AtomicInteger(0);
    }

    InternalKDLQProducerSession(@Nonnull final KDLQConfiguration configuration) {
        this(configuration.producerId(), new KafkaProducer<>(composePropertiesMap(configuration)));
    }

    private static Map<String, Object> composePropertiesMap(final KDLQConfiguration configuration) {
        final Map<String, Object> finalProperties = new HashMap<>(configuration.producerProperties());

        finalProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", configuration.bootstrapServers()));
        finalProperties.putIfAbsent(ProducerConfig.RETRIES_CONFIG, 5);
        finalProperties.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, 50);
        finalProperties.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        finalProperties.putIfAbsent(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        finalProperties.putIfAbsent(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        finalProperties.put(ProducerConfig.CLIENT_ID_CONFIG, KDLQ_ID);

        return finalProperties;
    }

    @Override
    public synchronized boolean close(
            @Nonnegative final int timeoutSeconds,
            @Nonnull final Runnable onCloseCallback
    ) {
        if (this.isClosed) {
            throw new KDLQLifecycleException("Producer already closed");
        } else if (this.usages.decrementAndGet() > 0) {
            return false;
        }

        this.isClosed = true;

        onCloseCallback.run();
        try {
            this.producer.close(Duration.ofSeconds(timeoutSeconds));
        } catch (RuntimeException ex) {
            logger.warn("Error on close producer: " + this.sessionId, ex);
        }

        return true;
    }

    @Override
    public synchronized boolean onUsage() {
        if (this.isClosed) {
            return false;
        }

        return this.usages.incrementAndGet() > 0;
    }

    @Override
    @Nonnull
    public String sessionId() {
        return this.sessionId;
    }

    @Override
    @Nonnull
    public Producer<K, V> producer() {
        return this.producer;
    }
}
