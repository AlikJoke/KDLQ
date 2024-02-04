package ru.joke.kdlq.core.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.core.KDLQConfiguration;
import ru.joke.kdlq.core.KDLQException;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class KDLQMessageSender implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(KDLQMessageSender.class);

    private static final String MESSAGE_KILLS_HEADER = "KDLQ_Kills";
    private static final int WAIT_TIMEOUT = 30;

    private static final KDLQMessageSender instance = new KDLQMessageSender();

    private final Map<String, ProducerSession<?, ?>> producersByConfiguration;
    private final KDLQHeadersService headersService;

    KDLQMessageSender() {
        this.producersByConfiguration = new ConcurrentHashMap<>();
        this.headersService = new KDLQHeadersService();
    }

    public <K, V> boolean sendIfNeed(@Nonnull ConsumerRecord<K, V> message, @Nonnull KDLQConfiguration dlqConfiguration) {
        final ProducerSession<K, V> producerSession = takeProducerSession(dlqConfiguration);

        try {
            synchronized (producerSession) {
                return tryToProduceMessageToDLQIfNeed(producerSession.producer, message, dlqConfiguration);
            }
        } catch (RuntimeException ex) {
            if (!producerSession.isClosed) {
                throw ex;
            }

            return sendIfNeed(message, dlqConfiguration);
        }
    }

    @Override
    public void close() {
        logger.info("Closing Kafka DLQ message producers");
        this.producersByConfiguration.values().forEach(ProducerSession::close);
        logger.info("Kafka DLQ message producers closed");
    }

    public void close(@Nonnull KDLQConfiguration dlqConfiguration) {
        final var configKey = getDLQConfigurationKey(dlqConfiguration);
        final var producerSession = takeProducerSession(dlqConfiguration);
        // TODO share producers without closing for all
        producerSession.close();
    }

    private <K, V> boolean tryToProduceMessageToDLQIfNeed(
            final Producer<K, V> producer,
            final ConsumerRecord<K, V> originalMessage,
            final KDLQConfiguration dlqConfiguration) {

        final int killsCounter = getNextKillsCounter(originalMessage.headers());
        if (killsCounter > dlqConfiguration.maxKills()) {
            logger.warn("Max kills count reached, message will be skipped");
            // TODO callback
            return false;
        }

        // TODO check processing tries before kill

        final ProducerRecord<K, V> dlqRecord = createRecord(originalMessage, dlqConfiguration, killsCounter);

        try {
            // TODO error handling, callbacks (on error and on success)
            producer.send(dlqRecord, (recordMetadata, e) -> {
                if (e != null) {
                    logger.error("Unable to send message to DLQ: " + dlqConfiguration.queueName(), e);
                }
            }).get(WAIT_TIMEOUT, TimeUnit.SECONDS);
        } catch (RetriableException | BrokerNotAvailableException | InterruptedException | ExecutionException | TimeoutException ex) {
            logger.error("Unable to send message to DLQ: " + dlqConfiguration.queueName(), ex);
            throw new KDLQException(ex);
        }

        return true;
    }

    private <K, V> ProducerSession<K, V> takeProducerSession(final KDLQConfiguration dlqConfiguration) {
        final var configKey = getDLQConfigurationKey(dlqConfiguration);
        @SuppressWarnings("unchecked")
        final var result = (ProducerSession<K, V>) this.producersByConfiguration.compute(
                configKey,
                (k, v) -> Objects.requireNonNullElseGet(v, () -> new ProducerSession<>(dlqConfiguration))
        );

        return result;
    }

    private String getDLQConfigurationKey(final KDLQConfiguration dlqConfiguration) {
        return String.valueOf(dlqConfiguration.hashCode());
    }

    private <K, V> ProducerRecord<K, V> createRecord(
            final ConsumerRecord<K, V> originalRecord,
            final KDLQConfiguration dlqConfiguration,
            final int nextKillsCounter) {

        final var headers = new RecordHeaders(originalRecord.headers().toArray());
        headers.add(this.headersService.createIntHeader(MESSAGE_KILLS_HEADER, nextKillsCounter));

        return new ProducerRecord<>(
                dlqConfiguration.queueName(),
                null,
                originalRecord.key(),
                originalRecord.value(),
                originalRecord.headers()
        );
    }

    private int getNextKillsCounter(final Headers originalHeaders) {
        return this.headersService.getIntHeader(MESSAGE_KILLS_HEADER, originalHeaders).orElse(0) + 1;
    }

    @Nonnull
    public static KDLQMessageSender getInstance() {
        return instance;
    }

    private static class ProducerSession<K, V> implements Closeable {

        private final Producer<K, V> producer;
        private volatile boolean isClosed;

        private ProducerSession(final KDLQConfiguration config) {
            // TODO additional hardcoded list of properties
            this.producer = new KafkaProducer<>(config.producerProperties());
        }

        @Override
        public synchronized void close() {
            if (this.isClosed) {
                return;
            }

            try {
                this.isClosed = true;
                this.producer.close(Duration.ofSeconds(WAIT_TIMEOUT));
            } catch (RuntimeException ex) {
                logger.warn("Error on close producer", ex);
            }
        }
    }
}
