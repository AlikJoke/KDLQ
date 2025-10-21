package ru.joke.kdlq.internal.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.*;
import ru.joke.kdlq.internal.routers.KDLQMessageRouter;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Default thread-safe implementation of the {@link KDLQMessageConsumer} that allows message
 * processing to be performed in accordance with the specified {@link KDLQConfiguration}
 * and the passed message processor {@link KDLQMessageProcessor}.<br>
 * Example of usage:<br>
 * <pre>
 *     {@code
 *         final var configuration = buildConfiguration();
 *         final var consumer = new ConfigurableKDLQMessageConsumer("test", configuration, message -> {
 *             if (!isMessageReadyToProcessing(message)) {
 *                 return ProcessingStatus.MUST_BE_REDELIVERED;
 *             }
 *             handleMessage(message);
 *             return ProcessingStatus.OK;
 *         });
 *
 *         final ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
 *         for (final ConsumerRecord<String, byte[]> record : records) {
 *             try {
 *                 final var status = consumer.accept(record);
 *                 logger.debug("Message {} processed with status {}", record, status);
 *             } catch (KDLQException ex) {
 *                 logger.error("Error while processing message", ex);
 *                 // rethrow or skip error
 *             }
 *         }
 *
 *         kafkaConsumer.commitSync();
 *     }
 * </pre>
 * This example does not contain correct error handling, rebalancing and working with commit offsets.
 *
 * @param <K> type of the message key
 * @param <V> type of the message body value
 * @author Alik
 * @see KDLQMessageConsumer
 * @see KDLQMessageProcessor
 * @see KDLQConfiguration
 */
@ThreadSafe
final class ConfigurableKDLQMessageConsumer<K, V> implements KDLQMessageConsumer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurableKDLQMessageConsumer.class);

    private final String id;
    private final KDLQMessageRouter<K, V> messageSender;
    private final KDLQConfiguration dlqConfiguration;
    private final KDLQMessageProcessor<K, V> messageProcessor;
    private final ReadWriteLock lock;
    private final Consumer<KDLQMessageConsumer<?, ?>> onCloseCallback;

    private volatile boolean isClosed;

    ConfigurableKDLQMessageConsumer(
            @Nonnull String id,
            @Nonnull KDLQConfiguration dlqConfiguration,
            @Nonnull KDLQMessageProcessor<K, V> messageProcessor,
            @Nonnull KDLQMessageRouter<K, V> messageSender,
            @Nonnull Consumer<KDLQMessageConsumer<?, ?>> onCloseCallback
    ) {
        this.id = Objects.requireNonNull(id, "id");
        this.dlqConfiguration = Objects.requireNonNull(dlqConfiguration, "dlqConfiguration");
        this.messageProcessor = Objects.requireNonNull(messageProcessor, "messageProcessor");
        this.lock = new ReentrantReadWriteLock();
        this.messageSender = Objects.requireNonNull(messageSender, "messageSender");
        this.onCloseCallback = Objects.requireNonNull(onCloseCallback, "onCloseCallback");
    }

    @Override
    @Nonnull
    public Status accept(@Nonnull ConsumerRecord<K, V> message) {

        final var sendingLock = this.lock.readLock();
        if (this.isClosed || !sendingLock.tryLock()) {
            throw new KDLQLifecycleException("Consumer already closed");
        }

        try {
            return handle(message);
        } finally {
            sendingLock.unlock();
        }
    }

    @Nonnull
    @Override
    public String id() {
        return this.id;
    }

    @Override
    public synchronized void close() {
        if (this.isClosed) {
            return;
        }

        final var closingLock = this.lock.writeLock();
        closingLock.lock();

        logger.info("Closing was called for KDLQ consumer: {}", this.id);

        try {
            this.isClosed = true;
            this.messageSender.close();
        } finally {
            closingLock.unlock();
        }

        this.onCloseCallback.accept(this);

        logger.info("KDLQ consumer was closed: {}", this.id);
    }

    @Override
    public String toString() {
        return "KDLQMessageConsumer{"
                + "id='" + id + '\''
                + ", dlqConfiguration=" + dlqConfiguration
                + '}';
    }

    private Status handle(final ConsumerRecord<K, V> message) {
        Objects.requireNonNull(message, "message");

        return switch (process(message)) {
            case OK -> Status.OK;
            case MUST_BE_REDELIVERED -> translateRoutingStatus(this.messageSender.routeToRedelivery(message));
            case MUST_BE_KILLED -> translateRoutingStatus(this.messageSender.routeToDLQ(message));
        };
    }

    private Status translateRoutingStatus(final KDLQMessageRouter.RoutingStatus routingStatus) {
        return switch (routingStatus) {
            case ROUTED_TO_DLQ -> Status.ROUTED_TO_DLQ;
            case SCHEDULED_TO_REDELIVERY -> Status.SCHEDULED_TO_REDELIVERY;
            case ROUTED_TO_REDELIVERY_QUEUE -> Status.ROUTED_TO_REDELIVERY_QUEUE;
            case DISCARDED -> Status.DISCARDED_DLQ_MAX_ATTEMPTS_REACHED;
        };
    }

    private KDLQMessageProcessor.ProcessingStatus process(final ConsumerRecord<K, V> message) {

        KDLQMessageProcessor.ProcessingStatus processingStatus;
        RuntimeException processingError = null;
        try {
            processingStatus = this.messageProcessor.process(message);
        } catch (KDLQMessageMustBeKilledException ex) {
            processingStatus = KDLQMessageProcessor.ProcessingStatus.MUST_BE_KILLED;
            processingError = ex;
        } catch (RuntimeException ex) {
            processingStatus = KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED;
            processingError = ex;
        }

        callListeners(message, processingStatus, processingError);

        return processingStatus;
    }

    private void callListeners(
            final ConsumerRecord<K, V> message,
            final KDLQMessageProcessor.ProcessingStatus processingStatus,
            final RuntimeException processingError
    ) {
        this.dlqConfiguration.lifecycleListeners().forEach(l -> l.onMessageProcessing(this.id, message, processingStatus, processingError));
    }
}
