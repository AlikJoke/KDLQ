package ru.joke.kdlq.impl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.*;
import ru.joke.kdlq.impl.internal.KDLQMessageSender;
import ru.joke.kdlq.impl.internal.KDLQMessageSenderFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
public final class ConfigurableKDLQMessageConsumer<K, V> implements KDLQMessageConsumer<K, V>, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurableKDLQMessageConsumer.class);

    private final String id;
    private final KDLQMessageSender<K, V> messageSender;
    private final KDLQConfiguration dlqConfiguration;
    private final KDLQMessageProcessor<K, V> messageProcessor;
    private final ReadWriteLock lock;

    private volatile boolean isClosed;

    public ConfigurableKDLQMessageConsumer(
            @Nonnull String id,
            @Nonnull KDLQConfiguration dlqConfiguration,
            @Nonnull KDLQMessageProcessor<K, V> messageProcessor) {
        this.id = Objects.requireNonNull(id, "id");
        this.dlqConfiguration = Objects.requireNonNull(dlqConfiguration, "dlqConfiguration");
        this.messageProcessor = Objects.requireNonNull(messageProcessor, "messageProcessor");
        this.lock = new ReentrantReadWriteLock();
        this.messageSender = KDLQMessageSenderFactory.getInstance().create(id, dlqConfiguration);
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

    @Override
    public synchronized void close() {
        if (this.isClosed) {
            throw new KDLQLifecycleException("Consumer already closed");
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

        logger.info("KDLQ consumer was closed: {}", this.id);
    }

    private Status handle(final ConsumerRecord<K, V> message) {
        Objects.requireNonNull(message, "message");

        return switch (process(message)) {
            case OK -> Status.OK;
            case MUST_BE_REDELIVERED -> this.messageSender.redeliver(message)
                                                                ? Status.REDELIVERED
                                                                : Status.ROUTED_TO_DLQ;
            case ERROR -> this.messageSender.sendToDLQ(message)
                                                ? Status.ROUTED_TO_DLQ
                                                : Status.SKIPPED_DLQ_MAX_ATTEMPTS_REACHED;
        };
    }

    private KDLQMessageProcessor.ProcessingStatus process(final ConsumerRecord<K, V> message) {

        KDLQMessageProcessor.ProcessingStatus processingStatus;
        RuntimeException processingError = null;
        try {
            processingStatus = this.messageProcessor.process(message);
        } catch (KDLQMessageMustBeRedeliveredException ex) {
            processingStatus = KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED;
            processingError = ex;
        } catch (RuntimeException ex) {
            processingStatus = KDLQMessageProcessor.ProcessingStatus.ERROR;
            processingError = ex;
        }

        callListeners(message, processingStatus, processingError);

        return processingStatus;
    }

    private void callListeners(
            final ConsumerRecord<K, V> message,
            final KDLQMessageProcessor.ProcessingStatus processingStatus,
            final RuntimeException processingError) {
        this.dlqConfiguration.lifecycleListeners().forEach(l -> {
            if (processingError != null) {
                l.onErrorMessageProcessing(this.id, message, processingStatus, processingError);
            } else {
                l.onSuccessMessageProcessing(this.id, message, processingStatus);
            }
        });
    }
}
