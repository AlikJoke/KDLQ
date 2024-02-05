package ru.joke.kdlq.core.internal;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.core.KDLQLifecycleException;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

final class KDLQProducerSession<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(KDLQProducerSession.class);

    private static final String KDLQ_ID = "KDLQ";

    private final Producer<K, V> producer;
    private final String sessionId;
    private final AtomicInteger usages;
    private volatile boolean isClosed;

    KDLQProducerSession(final String sessionId, final Map<String, Object> properties) {
        
        final Map<String, Object> finalProperties = new HashMap<>(properties);

        finalProperties.putIfAbsent(ProducerConfig.RETRIES_CONFIG, 5);
        finalProperties.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, 50);
        finalProperties.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        finalProperties.putIfAbsent(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        finalProperties.putIfAbsent(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        finalProperties.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, KDLQ_ID);

        this.producer = new KafkaProducer<>(finalProperties);
        this.sessionId = sessionId;
        this.usages = new AtomicInteger(0);
    }

    synchronized boolean close(final int timeoutSeconds, final Runnable onCloseCallback) {
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

    @Nonnull
    String sessionId() {
        return this.sessionId;
    }

    @Nonnull
    Producer<K, V> producer() {
        return this.producer;
    }

    synchronized boolean onUsage() {
        if (this.isClosed) {
            return false;
        }

        return this.usages.incrementAndGet() > 0;
    }
}
