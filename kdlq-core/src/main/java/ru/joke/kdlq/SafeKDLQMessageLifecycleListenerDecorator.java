package ru.joke.kdlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.internal.util.Args;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

final class SafeKDLQMessageLifecycleListenerDecorator implements KDLQMessageLifecycleListener {

    private final Logger logger;
    private final KDLQMessageLifecycleListener listener;

    SafeKDLQMessageLifecycleListenerDecorator(@Nonnull final KDLQMessageLifecycleListener listener) {
        this.listener = Args.requireNotNull(listener, () -> new KDLQConfigurationException("Provided null listener"));
        this.logger = LoggerFactory.getLogger(listener.getClass());
    }

    @Override
    public <K, V> void onMessageKillSuccess(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> dlqMessage
    ) {
        try {
            this.listener.onMessageKillSuccess(consumerId, originalMessage, dlqMessage);
        } catch (RuntimeException ex) {
            logger.warn("", ex);
        }
    }

    @Override
    public <K, V> void onMessageKillError(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> dlqMessage,
            @Nonnull Exception error
    ) {
        try {
            this.listener.onMessageKillError(consumerId, originalMessage, dlqMessage, error);
        } catch (RuntimeException ex) {
            logger.warn("", ex);
        }
    }

    @Override
    public <K, V> void onMessageRedeliverySuccess(
            @Nonnull String consumerId,
            @Nonnull Optional<ConsumerRecord<K, V>> originalMessage,
            @Nonnull ProducerRecord<K, V> messageToRedelivery
    ) {
        try {
            this.listener.onMessageRedeliverySuccess(consumerId, originalMessage, messageToRedelivery);
        } catch (RuntimeException ex) {
            logger.warn("", ex);
        }
    }

    @Override
    public <K, V> void onMessageRedeliveryError(
            @Nonnull String consumerId,
            @Nonnull Optional<ConsumerRecord<K, V>> originalMessage,
            @Nonnull ProducerRecord<K, V> messageToRedelivery,
            @Nonnull Exception error
    ) {
        try {
            this.listener.onMessageRedeliveryError(consumerId, originalMessage, messageToRedelivery, error);
        } catch (RuntimeException ex) {
            logger.warn("", ex);
        }
    }

    @Override
    public <K, V> void onDeferredMessageRedeliverySchedulingError(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> messageToRedelivery,
            @Nonnull Exception error
    ) {
        try {
            this.listener.onDeferredMessageRedeliverySchedulingError(consumerId, originalMessage, messageToRedelivery, error);
        } catch (RuntimeException ex) {
            logger.warn("", ex);
        }
    }

    @Override
    public <K, V> void onDeferredMessageRedeliverySchedulingSuccess(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> messageToRedelivery
    ) {
        try {
            this.listener.onDeferredMessageRedeliverySchedulingSuccess(consumerId, originalMessage, messageToRedelivery);
        } catch (RuntimeException ex) {
            logger.warn("", ex);
        }
    }

    @Override
    public <K, V> void onMessageDiscard(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> message
    ) {
        try {
            this.listener.onMessageDiscard(consumerId, message);
        } catch (RuntimeException ex) {
            logger.warn("", ex);
        }
    }

    @Override
    public <K, V> void onMessageProcessing(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> message,
            @Nonnull KDLQMessageProcessor.ProcessingStatus processingStatus,
            @Nullable RuntimeException error
    ) {
        try {
            this.listener.onMessageProcessing(consumerId, message, processingStatus, error);
        } catch (RuntimeException ex) {
            logger.warn("", ex);
        }
    }
}
