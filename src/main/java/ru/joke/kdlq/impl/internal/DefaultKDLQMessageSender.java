package ru.joke.kdlq.impl.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQException;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

final class DefaultKDLQMessageSender<K, V> implements KDLQMessageSender<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKDLQMessageSender.class);

    private static final String MESSAGE_KILLS_HEADER = "KDLQ_Kills";
    private static final String MESSAGE_PRC_MARKER_HEADER = "KDLQ_ProcessingMarker";
    private static final String MESSAGE_REDELIVERY_ATTEMPTS_HEADER = "KDLQ_Redelivered";

    private static final int WAIT_TIMEOUT = 30;

    private final KDLQHeadersService headersService;
    private final KDLQConfiguration dlqConfiguration;
    private final String producerId;
    private final String sourceProcessorId;
    private final KDLQProducerSession<K, V> producerSession;
    private final KDLQProducersRegistry producersRegistry;

    DefaultKDLQMessageSender(@Nonnull String sourceProcessorId, @Nonnull KDLQConfiguration dlqConfiguration) {
        this.dlqConfiguration = dlqConfiguration;
        this.sourceProcessorId = sourceProcessorId;
        this.producerId = dlqConfiguration.id();
        this.headersService = new KDLQHeadersService();
        this.producersRegistry = KDLQProducersRegistryHolder.get();
        this.producerSession = createProducerSession();
    }

    @Override
    public boolean redeliver(@Nonnull ConsumerRecord<K, V> originalMessage) {

        final var listeners = this.dlqConfiguration.lifecycleListeners();
        final int redelivered = getNextRedeliveryAttemptsCounter(originalMessage.headers());
        final int maxRedeliveryAttempts = dlqConfiguration.maxRedeliveryAttemptsBeforeKill();
        if (maxRedeliveryAttempts >= 0 && redelivered > maxRedeliveryAttempts) {
            logger.warn("Max redelivery attempts count reached, message will be routed to DLQ");

            return sendToDLQ(originalMessage);
        }

        final String targetQueue = this.dlqConfiguration.redeliveryQueueName() == null ? originalMessage.topic() : this.dlqConfiguration.redeliveryQueueName();
        final ProducerRecord<K, V> recordToRedelivery = createRecord(
                originalMessage,
                MESSAGE_REDELIVERY_ATTEMPTS_HEADER,
                redelivered,
                targetQueue
        );

        try {
            send(recordToRedelivery);
        } catch (Exception ex) {
            logger.error("Unable to redeliver message: " + targetQueue, ex);
            listeners.forEach(l -> l.onMessageRedeliveryError(this.sourceProcessorId, originalMessage, recordToRedelivery, ex));

            throw new KDLQException(ex);
        }

        listeners.forEach(l -> l.onMessageRedeliverySuccess(this.sourceProcessorId, originalMessage, recordToRedelivery));

        return true;
    }

    @Override
    public boolean sendToDLQ(@Nonnull ConsumerRecord<K, V> originalMessage) {

        final var listeners = this.dlqConfiguration.lifecycleListeners();
        final int killsCounter = getNextRedeliveryAttemptsCounter(originalMessage.headers());
        final int maxKills = dlqConfiguration.maxKills();
        if (maxKills >= 0 && killsCounter > maxKills) {
            logger.warn("Max kills count reached, message will be skipped");
            listeners.forEach(l -> l.onMessageSkip(this.sourceProcessorId, originalMessage));

            return false;
        }

        final ProducerRecord<K, V> dlqRecord = createRecord(
                originalMessage,
                MESSAGE_KILLS_HEADER,
                killsCounter,
                this.dlqConfiguration.deadLetterQueueName()
        );

        try {
            send(dlqRecord);
        } catch (Exception ex) {
            logger.error("Unable to send message to DLQ: " + dlqConfiguration.deadLetterQueueName(), ex);
            listeners.forEach(l -> l.onMessageKillError(this.sourceProcessorId, originalMessage, dlqRecord, ex));

            throw new KDLQException(ex);
        }

        listeners.forEach(l -> l.onMessageKillSuccess(this.sourceProcessorId, originalMessage, dlqRecord));

        return true;
    }

    @Override
    public void close() {
        final var producerSession = this.producersRegistry.get(this.producerId);
        producerSession
                .stream()
                .peek(session -> logger.info("Closing KDLQ message producer"))
                .filter(session -> session.close(WAIT_TIMEOUT, () -> this.producersRegistry.unregister(session.sessionId())))
                .findAny()
                .ifPresent(session -> logger.info("KDLQ message producer closed"));
    }

    private void send(final ProducerRecord<K, V> record) throws Exception {
        this.producerSession.producer().send(record, (recordMetadata, e) -> {
            if (e != null) {
                throw new KDLQException(e);
            }
        }).get(WAIT_TIMEOUT, TimeUnit.SECONDS);
    }

    private KDLQProducerSession<K, V> createProducerSession() {
        final KDLQProducerSession<K, V> session = this.producersRegistry.registerIfNeed(
                this.producerId,
                () -> new KDLQProducerSession<>(this.dlqConfiguration)
        );

        if (!session.onUsage()) {
            return createProducerSession();
        }

        return session;
    }

    private ProducerRecord<K, V> createRecord(
            final ConsumerRecord<K, V> originalRecord,
            final String counterHeader,
            final int counterValue,
            final String targetQueue) {

        final var headers = new RecordHeaders(originalRecord.headers().toArray());
        headers.add(this.headersService.createIntHeader(counterHeader, counterValue));
        headers.add(this.headersService.createStringHeader(MESSAGE_PRC_MARKER_HEADER, this.sourceProcessorId));

        return new ProducerRecord<>(
                targetQueue,
                null,
                originalRecord.key(),
                originalRecord.value(),
                headers
        );
    }

    private int getNextKillsCounter(final Headers originalHeaders) {
        return getNextCounterHeader(originalHeaders, MESSAGE_KILLS_HEADER);
    }

    private int getNextRedeliveryAttemptsCounter(final Headers originalHeaders) {
        return getNextCounterHeader(originalHeaders, MESSAGE_REDELIVERY_ATTEMPTS_HEADER);
    }

    private int getNextCounterHeader(final Headers originalHeaders, final String header) {
        return this.headersService.getIntHeader(header, originalHeaders).orElse(0) + 1;
    }
}
