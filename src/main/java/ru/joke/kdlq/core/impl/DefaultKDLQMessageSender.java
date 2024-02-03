package ru.joke.kdlq.core.impl;

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
import ru.joke.kdlq.core.KDLQMessageSender;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class DefaultKDLQMessageSender implements KDLQMessageSender, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(KDLQMessageSender.class);

    static final String MESSAGE_KILLS_HEADER = "KDLQ_Kills";
    private static final int WAIT_TIMEOUT = 30;

    private final Map<String, ProducerSession<?, ?>> producersByConfiguration;
    private final KDLQHeadersService kdlqHeadersService;

    public DefaultKDLQMessageSender() {
        this.producersByConfiguration = new ConcurrentHashMap<>();
        this.kdlqHeadersService = new KDLQHeadersService();
    }

    @Override
    public <K, V> void send(@Nonnull ConsumerRecord<K, V> message, @Nonnull KDLQConfiguration dlqConfiguration) {
        final Producer<K, V> producer = takeProducer(dlqConfiguration);
        tryToProduceMessageToDLQ(producer, message, dlqConfiguration);
    }

    @Override
    public void close() {
        logger.info("Closing Kafka DLQ message producers");
        this.producersByConfiguration.values().forEach(ProducerSession::close);
        logger.info("Kafka DLQ message producers closed");
    }

    private <K, V> void tryToProduceMessageToDLQ(
            final Producer<K, V> producer,
            final ConsumerRecord<K, V> originalMessage,
            final KDLQConfiguration dlqConfiguration) {

        final ProducerRecord<K, V> dlqRecord = createRecord(originalMessage, dlqConfiguration);

        try {
            producer.send(dlqRecord, (recordMetadata, e) -> {
                if (e != null) {
                    logger.error("Unable to send message to DLQ: " + dlqConfiguration.queueName(), e);
                }
            }).get(WAIT_TIMEOUT, TimeUnit.SECONDS);
        } catch (RetriableException | BrokerNotAvailableException | InterruptedException | ExecutionException | TimeoutException ex) {
            logger.error("Unable to send message to DLQ: " + dlqConfiguration.queueName(), ex);
            throw new KDLQException(ex);
        }
    }

    private <K, V> Producer<K, V> takeProducer(final KDLQConfiguration dlqConfiguration) {
        final String dlqConfigHash = String.valueOf(dlqConfiguration.hashCode());
        @SuppressWarnings("unchecked")
        final var result = (Producer<K, V>) this.producersByConfiguration.compute(
                dlqConfigHash,
                (k, v) -> Objects.requireNonNullElseGet(v, () -> new ProducerSession<>(dlqConfiguration))
        ).producer;

        return result;
    }

    private <K, V> ProducerRecord<K, V> createRecord(final ConsumerRecord<K, V> originalRecord, final KDLQConfiguration dlqConfiguration) {

        final var headers = new RecordHeaders(originalRecord.headers().toArray());
        headers.add(this.kdlqHeadersService.createIntHeader(MESSAGE_KILLS_HEADER, getNextKillsCounter(headers)));

        return new ProducerRecord<>(
                dlqConfiguration.queueName(),
                null,
                originalRecord.key(),
                originalRecord.value(),
                originalRecord.headers()
        );
    }

    private int getNextKillsCounter(final Headers originalHeaders) {
        return this.kdlqHeadersService.getIntHeader(MESSAGE_KILLS_HEADER, originalHeaders).orElse(0) + 1;
    }

    private static class ProducerSession<K, V> implements Closeable {

        private final Producer<K, V> producer;

        private ProducerSession(final KDLQConfiguration config) {
            this.producer = new KafkaProducer<>(config.producerProperties());
        }

        @Override
        public void close() {
            try {
                this.producer.close(Duration.ofSeconds(WAIT_TIMEOUT));
            } catch (RuntimeException ex) {
                logger.warn("Error on close producer", ex);
            }
        }
    }
}
