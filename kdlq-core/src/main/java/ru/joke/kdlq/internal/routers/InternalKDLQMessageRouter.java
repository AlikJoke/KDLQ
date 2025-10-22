package ru.joke.kdlq.internal.routers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQException;
import ru.joke.kdlq.KDLQProducerRecord;
import ru.joke.kdlq.internal.routers.headers.KDLQHeadersService;
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducer;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static ru.joke.kdlq.internal.routers.headers.KDLQHeaders.*;

final class InternalKDLQMessageRouter<K, V> implements KDLQMessageRouter<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(InternalKDLQMessageRouter.class);

    private static final int WAIT_TIMEOUT = 30;

    private final KDLQHeadersService headersService;
    private final KDLQConfiguration dlqConfiguration;
    private final String sourceProcessorId;
    private final Supplier<KDLQRedeliveryStorage> redeliveryStorageFactory;
    private final KDLQMessageProducer<K, V> messageSender;
    
    InternalKDLQMessageRouter(
            @Nonnull String sourceProcessorId,
            @Nonnull KDLQConfiguration dlqConfiguration,
            @Nonnull KDLQMessageProducer<K, V> messageSender,
            @Nonnull Supplier<KDLQRedeliveryStorage> redeliveryStorageFactory,
            @Nonnull KDLQHeadersService headersService
    ) {
        this.dlqConfiguration = dlqConfiguration;
        this.sourceProcessorId = sourceProcessorId;
        this.messageSender = messageSender;
        this.headersService = headersService;
        this.redeliveryStorageFactory = redeliveryStorageFactory;
    }

    @Override
    @Nonnull
    public RoutingStatus routeToRedelivery(@Nonnull ConsumerRecord<K, V> originalMessage) {

        final var listeners = this.dlqConfiguration.lifecycleListeners();
        final int redelivered = getNextRedeliveryAttemptsCounter(originalMessage.headers());
        final var redeliveryConfig = this.dlqConfiguration.redelivery();
        final int maxRedeliveryAttempts = redeliveryConfig.maxRedeliveryAttemptsBeforeKill();
        if (maxRedeliveryAttempts >= 0 && redelivered > maxRedeliveryAttempts) {
            logger.warn("Max redelivery attempts count reached, message will be routed to DLQ");
            routeToDLQ(originalMessage);

            return RoutingStatus.ROUTED_TO_DLQ;
        }

        final String targetQueue = redeliveryConfig.redeliveryQueueName() == null ? originalMessage.topic() : redeliveryConfig.redeliveryQueueName();
        final ProducerRecord<K, V> recordToRedelivery = createRecord(
                originalMessage,
                MESSAGE_REDELIVERY_ATTEMPTS_HEADER,
                redelivered,
                targetQueue
        );

        if (this.dlqConfiguration.redelivery().redeliveryDelay() > 0) {
            final var storage = this.redeliveryStorageFactory.get();
            if (storage != null) {
                final var recordToStore = createRecordToRedelivery(recordToRedelivery, redelivered);

                try {
                    storage.store(recordToStore);
                } catch (RuntimeException ex) {
                    logger.error("Unable to schedule message redelivery to " + targetQueue, ex);
                    listeners.forEach(
                            l -> l.onDeferredMessageRedeliverySchedulingError(
                                    this.sourceProcessorId,
                                    originalMessage,
                                    recordToRedelivery,
                                    ex
                            )
                    );

                    throw new KDLQException(ex);
                }

                listeners.forEach(
                        l -> l.onDeferredMessageRedeliverySchedulingSuccess(
                                this.sourceProcessorId,
                                originalMessage,
                                recordToRedelivery
                        )
                );

                return RoutingStatus.SCHEDULED_TO_REDELIVERY;
            }
        }

        try {
            this.messageSender.send(recordToRedelivery);
        } catch (Exception ex) {
            logger.error("Unable to redeliver message: " + targetQueue, ex);
            listeners.forEach(l -> l.onMessageRedeliveryError(this.sourceProcessorId, Optional.of(originalMessage), recordToRedelivery, ex));

            throw new KDLQException(ex);
        }

        listeners.forEach(l -> l.onMessageRedeliverySuccess(this.sourceProcessorId, Optional.of(originalMessage), recordToRedelivery));

        return RoutingStatus.ROUTED_TO_REDELIVERY_QUEUE;
    }

    @Override
    @Nonnull
    public RoutingStatus routeToDLQ(@Nonnull ConsumerRecord<K, V> originalMessage) {

        final var listeners = this.dlqConfiguration.lifecycleListeners();
        final int killsCounter = getNextKillsCounter(originalMessage.headers());
        final var dlqConfig = this.dlqConfiguration.dlq();
        final int maxKills = dlqConfig.maxKills();
        if (maxKills >= 0 && killsCounter > maxKills) {
            logger.warn("Max kills count reached, message will be skipped");
            listeners.forEach(l -> l.onMessageDiscard(this.sourceProcessorId, originalMessage));

            return RoutingStatus.DISCARDED;
        }

        final ProducerRecord<K, V> dlqRecord = createRecord(
                originalMessage,
                MESSAGE_KILLS_HEADER,
                killsCounter,
                dlqConfig.deadLetterQueueName()
        );

        try {
            this.messageSender.send(dlqRecord);
        } catch (Exception ex) {
            logger.error("Unable to send message to DLQ: " + dlqConfig.deadLetterQueueName(), ex);
            listeners.forEach(l -> l.onMessageKillError(this.sourceProcessorId, originalMessage, dlqRecord, ex));

            throw new KDLQException(ex);
        }

        listeners.forEach(l -> l.onMessageKillSuccess(this.sourceProcessorId, originalMessage, dlqRecord));

        return RoutingStatus.ROUTED_TO_DLQ;
    }

    @Override
    public void close() {
        this.messageSender.close();
    }

    private KDLQProducerRecord<byte[], byte[]> createRecordToRedelivery(
            final ProducerRecord<K, V> record,
            final int redeliveryAttempt
    ) {
        final var redeliveryConfig = this.dlqConfiguration.redelivery();
        final long redeliveryDelayMs = (long) (redeliveryConfig.redeliveryDelay() * Math.pow(redeliveryConfig.redeliveryDelayMultiplier(), redeliveryAttempt - 1));
        final long nextRedeliveryTimestamp = System.currentTimeMillis() + redeliveryDelayMs;

        // TODO record to redelivery
        final ProducerRecord<byte[], byte[]> recordToRedelivery = null;
        return new DefaultKDLQProducerRecord<>(
                UUID.randomUUID().toString(),
                recordToRedelivery,
                this.dlqConfiguration,
                nextRedeliveryTimestamp
        );
    }

    private ProducerRecord<K, V> createRecord(
            final ConsumerRecord<K, V> originalRecord,
            final String counterHeader,
            final int counterValue,
            final String targetQueue
    ) {

        final var headers = new RecordHeaders(originalRecord.headers().toArray());
        headers.add(this.headersService.createIntHeader(counterHeader, counterValue));
        headers.add(this.headersService.createStringHeader(MESSAGE_PRC_MARKER_HEADER, this.sourceProcessorId));
        if (this.dlqConfiguration.addOptionalInformationalHeaders()) {
            headers.add(this.headersService.createLongHeader(MESSAGE_TIMESTAMP_HEADER, originalRecord.timestamp()));
            headers.add(this.headersService.createLongHeader(MESSAGE_OFFSET_HEADER, originalRecord.offset()));
            headers.add(this.headersService.createIntHeader(MESSAGE_PARTITION_HEADER, originalRecord.partition()));
        }

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

    public record DefaultKDLQProducerRecord<K, V>(
            @Nonnull String id,
            @Nonnull ProducerRecord<K, V> record,
            @Nonnull KDLQConfiguration configuration,
            @Nonnegative long nextRedeliveryTimestamp
    ) implements KDLQProducerRecord<K, V> {
    }
}
