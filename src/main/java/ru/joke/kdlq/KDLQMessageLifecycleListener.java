package ru.joke.kdlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
     * @param consumerId id of the source processor to mark message, can not be {@code null}.
     * @param originalMessage source message, can not be {@code null}.
     * @param dlqMessage message to send to DLQ, can not be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     */
    default <K, V> void onMessageKillSuccess(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> dlqMessage) {
    }

    /**
     * Called if an error occurs when trying to send message to the DLQ.
     *
     * @param consumerId id of the source processor to mark message, can not be {@code null}.
     * @param originalMessage source message, can not be {@code null}.
     * @param dlqMessage message to send to DLQ, can not be {@code null}.
     * @param error error which occurs when trying to send to the DLQ, can not be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     */
    default <K, V> void onMessageKillError(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> dlqMessage,
            @Nonnull Exception error) {
    }

    /**
     * Called when a message is successfully delivered to the redelivery queue.
     *
     * @param consumerId id of the source processor to mark message, can not be {@code null}.
     * @param originalMessage source message, can not be {@code null}.
     * @param messageToRedelivery message to send to redelivery queue, can not be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     */
    default <K, V> void onMessageRedeliverySuccess(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> messageToRedelivery) {
    }

    /**
     * Called if an error occurs when trying to send message to the redelivery queue.
     *
     * @param consumerId id of the source processor to mark message, can not be {@code null}.
     * @param originalMessage source message, can not be {@code null}.
     * @param messageToRedelivery message to send to redelivery queue, can not be {@code null}.
     * @param error error which occurs when trying to send to the redelivery queue, can not be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     */
    default <K, V> void onMessageRedeliveryError(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> messageToRedelivery,
            @Nonnull Exception error) {
    }

    /**
     * Called when a message has been skipped as a result of exceeding the number of attempts
     * to send a message to the DLQ.
     *
     * @param consumerId id of the source processor to mark message, can not be {@code null}.
     * @param message source message, can not be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     */
    default <K, V> void onMessageSkip(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> message) {
    }

    /**
     * Called when a message is processed by the application handler.
     *
     * @param consumerId id of the source processor to mark message, can not be {@code null}.
     * @param message source message, can not be {@code null}.
     * @param processingStatus status of the processing, can not be {@code null}.
     * @param <K> type of the message key
     * @param <V> type of the message body value
     * @see KDLQMessageProcessor.ProcessingStatus
     */
    default <K, V> void onMessageProcessing(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> message,
            @Nonnull KDLQMessageProcessor.ProcessingStatus processingStatus,
            @Nullable RuntimeException error) {
    }
}
