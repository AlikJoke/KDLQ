package ru.joke.kdlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Message lifecycle listener. As various actions are performed on messages, listener callbacks
 * are executed to indicate the success or failure of a particular lifecycle step.
 *
 * @author Alik
 * @see KDLQConfiguration
 */
public interface KDLQMessageLifecycleListener {

    /**
     * Called when a message is successfully sent to the DLQ.
     *
     * @param consumerId      id of the source processor to mark message; cannot be {@code null}.
     * @param originalMessage source message; cannot be {@code null}.
     * @param dlqMessage      message to send to DLQ; cannot be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     */
    default <K, V> void onMessageKillSuccess(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> dlqMessage
    ) {
    }

    /**
     * Called if an error occurs when trying to send message to the DLQ.
     *
     * @param consumerId      id of the source processor to mark message; cannot be {@code null}.
     * @param originalMessage source message; cannot be {@code null}.
     * @param dlqMessage      message to send to DLQ; cannot be {@code null}.
     * @param error           error which occurs when trying to send to the DLQ; cannot be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     */
    default <K, V> void onMessageKillError(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> dlqMessage,
            @Nonnull Exception error
    ) {
    }

    /**
     * Called when a message is successfully sent to the redelivery queue.<br>
     *
     * @param consumerId          id of the source processor to mark message; cannot be {@code null}.
     * @param originalMessage     source message; cannot be {@code null}; if redelivery is deferred
     *                            (when redelivery delay is specified) then original message is not available (will be {@link Optional#empty()}).
     * @param messageToRedelivery message to send to redelivery queue; cannot be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     */
    default <K, V> void onMessageRedeliverySuccess(
            @Nonnull String consumerId,
            @Nonnull Optional<ConsumerRecord<K, V>> originalMessage,
            @Nonnull ProducerRecord<K, V> messageToRedelivery
    ) {
    }

    /**
     * Called if an error occurs when trying to send message to the redelivery queue.
     *
     * @param consumerId          id of the source processor to mark message; cannot be {@code null}.
     * @param originalMessage     source message; cannot be {@code null}; if redelivery is deferred
     *                            (when redelivery delay is specified) then original message is not available (will be {@link Optional#empty()}).
     * @param messageToRedelivery message to send to redelivery queue; cannot be {@code null}.
     * @param error error which occurs when trying to send to the redelivery queue; cannot be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     */
    default <K, V> void onMessageRedeliveryError(
            @Nonnull String consumerId,
            @Nonnull Optional<ConsumerRecord<K, V>> originalMessage,
            @Nonnull ProducerRecord<K, V> messageToRedelivery,
            @Nonnull Exception error
    ) {
    }

    /**
     * Called when an error occurs during the attempt to store a message in the redelivery storage
     * for later redelivery (this is called if delayed redelivery is enabled and a redelivery storage
     * is configured).
     *
     * @param consumerId          id of the source processor to mark message; cannot be {@code null}.
     * @param originalMessage     source message; cannot be {@code null}.
     * @param messageToRedelivery message to store to redelivery storage; cannot be {@code null}.
     * @param error               error which occurs when trying to store message to the redelivery storage; cannot be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     */
    default <K, V> void onDeferredMessageRedeliverySchedulingError(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> messageToRedelivery,
            @Nonnull Exception error
    ) {
    }

    /**
     * Called when a message is successfully stored in the redelivery storage for later redelivery
     * (this is called if delayed redelivery is enabled and a redelivery storage is configured).
     *
     * @param consumerId          id of the source processor to mark message; cannot be {@code null}.
     * @param originalMessage     source message; cannot be {@code null}.
     * @param messageToRedelivery message to store to redelivery storage; cannot be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     */
    default <K, V> void onDeferredMessageRedeliverySchedulingSuccess(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> messageToRedelivery
    ) {
    }

    /**
     * Called when a message has been discarded as a result of exceeding the number of attempts
     * to send a message to the DLQ.
     *
     * @param consumerId id of the source processor to mark message; cannot be {@code null}.
     * @param message    source message; cannot be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     */
    default <K, V> void onMessageDiscard(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> message
    ) {
    }

    /**
     * Called when a message is processed by the application handler.
     *
     * @param consumerId       id of the source processor to mark message; cannot be {@code null}.
     * @param message          source message; cannot be {@code null}.
     * @param processingStatus status of the processing; cannot be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     * @see KDLQMessageProcessor.ProcessingStatus
     */
    default <K, V> void onMessageProcessing(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> message,
            @Nonnull KDLQMessageProcessor.ProcessingStatus processingStatus,
            @Nullable RuntimeException error
    ) {
    }
}
