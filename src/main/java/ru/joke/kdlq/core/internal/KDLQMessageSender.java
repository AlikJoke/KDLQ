package ru.joke.kdlq.core.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.core.KDLQConfiguration;
import ru.joke.kdlq.core.KDLQException;
import ru.joke.kdlq.core.KDLQLifecycleException;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public final class KDLQMessageSender<K, V> implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(KDLQMessageSender.class);

    private static final String MESSAGE_KILLS_HEADER = "KDLQ_Kills";
    private static final String MESSAGE_PRC_MARKER_HEADER = "KDLQ_ProcessingMarker";

    private static final int WAIT_TIMEOUT = 30;

    private final KDLQHeadersService headersService;
    private final KDLQConfiguration dlqConfiguration;
    private final String producerId;
    private final String sourceProcessorId;
    private final KDLQProducerSession<K, V> producerSession;
    private final KDLQProducersRegistry producersRegistry;

    private volatile boolean isClosed;

    public KDLQMessageSender(@Nonnull String sourceProcessorId, @Nonnull KDLQConfiguration dlqConfiguration) {
        this.dlqConfiguration = dlqConfiguration;
        this.sourceProcessorId = sourceProcessorId;
        this.producerId = String.valueOf(dlqConfiguration.hashCode());
        this.headersService = new KDLQHeadersService();
        this.producersRegistry = KDLQProducersRegistryHolder.get();
        this.producerSession = createProducerSession();
    }

    public boolean redeliver(@Nonnull ConsumerRecord<K, V> message) {

        // TODO
        return false;
    }

    public boolean sendToDLQ(@Nonnull ConsumerRecord<K, V> originalMessage) {
        final var listeners = this.dlqConfiguration.lifecycleListeners();
        final int killsCounter = getNextKillsCounter(originalMessage.headers());
        if (killsCounter > dlqConfiguration.maxKills()) {
            logger.warn("Max kills count reached, message will be skipped");
            listeners.forEach(l -> l.onMessageSkip(this.sourceProcessorId, originalMessage));

            return false;
        }

        final ProducerRecord<K, V> dlqRecord = createRecord(originalMessage, dlqConfiguration, killsCounter);

        try {
            this.producerSession.producer().send(dlqRecord, (recordMetadata, e) -> {
                if (e != null) {
                    throw new KDLQException(e);
                }
            }).get(WAIT_TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception ex) {
            logger.error("Unable to send message to DLQ: " + dlqConfiguration.deadLetterQueueName(), ex);
            listeners.forEach(l -> l.onMessageKillError(this.sourceProcessorId, originalMessage, dlqRecord, ex));

            throw new KDLQException(ex);
        }

        listeners.forEach(l -> l.onMessageKillSuccess(this.sourceProcessorId, originalMessage, dlqRecord));

        return true;
    }

    @Override
    public synchronized void close() {
        if (this.isClosed) {
            throw new KDLQLifecycleException("Sender already closed");
        }

        this.isClosed = true;

        final var producerSession = this.producersRegistry.get(this.producerId);
        producerSession
                .stream()
                .peek(session -> logger.info("Closing Kafka DLQ message producer"))
                .filter(session -> session.close(WAIT_TIMEOUT, () -> this.producersRegistry.unregister(session.sessionId())))
                .findAny()
                .ifPresent(session -> logger.info("Kafka DLQ message producer closed"));
    }

    private KDLQProducerSession<K, V> createProducerSession() {
        final KDLQProducerSession<K, V> session = this.producersRegistry.registerIfNeed(
                this.producerId,
                () -> new KDLQProducerSession<>(this.producerId, dlqConfiguration.producerProperties())
        );

        if (!session.onUsage()) {
            return createProducerSession();
        }

        return session;
    }

    private ProducerRecord<K, V> createRecord(
            final ConsumerRecord<K, V> originalRecord,
            final KDLQConfiguration dlqConfiguration,
            final int nextKillsCounter) {

        final var headers = new RecordHeaders(originalRecord.headers().toArray());
        headers.add(this.headersService.createIntHeader(MESSAGE_KILLS_HEADER, nextKillsCounter));
        headers.add(this.headersService.createStringHeader(MESSAGE_PRC_MARKER_HEADER, this.sourceProcessorId));

        return new ProducerRecord<>(
                dlqConfiguration.deadLetterQueueName(),
                null,
                originalRecord.key(),
                originalRecord.value(),
                headers
        );
    }

    private int getNextKillsCounter(final Headers originalHeaders) {
        return this.headersService.getIntHeader(MESSAGE_KILLS_HEADER, originalHeaders).orElse(0) + 1;
    }
}
