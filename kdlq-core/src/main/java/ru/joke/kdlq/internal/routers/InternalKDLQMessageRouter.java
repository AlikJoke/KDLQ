package ru.joke.kdlq.internal.routers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQConfigurationException;
import ru.joke.kdlq.KDLQException;
import ru.joke.kdlq.KDLQProducerRecord;
import ru.joke.kdlq.internal.routers.headers.KDLQHeadersService;
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducer;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static ru.joke.kdlq.internal.routers.headers.KDLQHeaders.*;

@ThreadSafe
final class InternalKDLQMessageRouter<K, V> implements KDLQMessageRouter<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(InternalKDLQMessageRouter.class);

    private static final int WAIT_TIMEOUT = 30;

    private final KDLQHeadersService headersService;
    private final KDLQConfiguration dlqConfiguration;
    private final String sourceProcessorId;
    private final Supplier<KDLQRedeliveryStorage> redeliveryStorageFactory;
    private final KDLQMessageProducer<K, V> messageSender;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    
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
        this.keySerializer = createSerializer(dlqConfiguration, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        this.valueSerializer = createSerializer(dlqConfiguration, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
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
            routeToRedeliveryStorage(originalMessage, recordToRedelivery, redelivered);
            return RoutingStatus.SCHEDULED_TO_REDELIVERY;
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

    private void routeToRedeliveryStorage(
            final ConsumerRecord<K, V> originalMessage,
            final ProducerRecord<K, V> recordToRedelivery,
            final int redeliveryAttempt
    ) {
        final var listeners = this.dlqConfiguration.lifecycleListeners();
        final var recordToStore = createRecordToRedelivery(recordToRedelivery, redeliveryAttempt);

        try {
            final var storage = this.redeliveryStorageFactory.get();
            if (storage == null) {
                throw new KDLQConfigurationException("Invalid configuration: redelivery delay was set but redelivery storage isn't configured");
            }

            storage.store(recordToStore);
        } catch (RuntimeException ex) {
            logger.error("Unable to save message for redelivery to storage", ex);
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
    }

    private KDLQProducerRecord<byte[], byte[]> createRecordToRedelivery(
            final ProducerRecord<K, V> record,
            final int redeliveryAttempt
    ) {
        final var redeliveryConfig = this.dlqConfiguration.redelivery();
        final var currentRedeliveryDelay = (long) (redeliveryConfig.redeliveryDelay() * Math.pow(redeliveryConfig.redeliveryDelayMultiplier(), redeliveryAttempt - 1));
        final long resultRedeliveryDelayMs = Math.min(redeliveryConfig.maxRedeliveryDelay(), currentRedeliveryDelay);
        final long nextRedeliveryTimestamp = System.currentTimeMillis() + resultRedeliveryDelayMs;

        final var keyData = this.keySerializer.serialize(record.topic(), record.headers(), record.key());
        final var valueData = this.valueSerializer.serialize(record.topic(), record.headers(), record.value());

        final ProducerRecord<byte[], byte[]> recordToRedelivery = new ProducerRecord<>(
                record.topic(),
                null,
                keyData,
                valueData,
                record.headers()
        );

        return new DefaultKDLQProducerRecord<>(
                UUID.randomUUID().toString(),
                recordToRedelivery,
                this.dlqConfiguration,
                nextRedeliveryTimestamp
        );
    }

    private static <T> Serializer<T> createSerializer(
            final KDLQConfiguration dlqConfiguration,
            final String configId
    ) {
        final var serializerConfig = dlqConfiguration.producerProperties().get(configId);
        if (serializerConfig == null) {
            throw new KDLQException("Serializer property " + configId + " not found in producer properties");
        }

        final String serializerClassName;
        if (serializerConfig instanceof Class<?> serializerClass) {
            serializerClassName = serializerClass.getName();
        } else if (serializerConfig instanceof String serializerName) {
            serializerClassName = serializerName;
        } else {
            throw new KDLQException("Invalid serializer class value type for %s: %s".formatted(configId, serializerConfig.getClass().getName()));
        }

        try {
            final var serializerClass = Class.forName(serializerClassName, true, ProducerRecord.class.getClassLoader());
            if (!Serializer.class.isAssignableFrom(serializerClass)) {
                throw new KDLQException("Class " + serializerClassName + " does not implement " + Serializer.class.getName());
            }

            @SuppressWarnings("unchecked")
            final var serializer = (Serializer<T>) serializerClass.getDeclaredConstructor().newInstance();
            serializer.configure(dlqConfiguration.producerProperties(), configId.equals(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));

            return serializer;
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
            throw new KDLQException(ex);
        }
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
