package ru.joke.kdlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nonnull;

/**
 * A message consumer that processes messages and resends messages when necessary or
 * sends them to the DLQ according to the specified KDLQ configuration.
 *
 * @param <K> type of the message key
 * @param <V> type of the message body value
 * @author Alik
 * @see KDLQMessageProcessor
 * @see KDLQConfiguration
 */
public interface KDLQMessageConsumer<K, V> {

    /**
     * Accepts Kafka message and processes it. If processing is successful, it does not perform
     * any further actions; if redelivery is necessary, it submits the message for redelivery,
     * and in case of unexpected errors, it causes the message to be sent to the DLQ according
     * to the specified KDLQ configuration.
     *
     * @param message Kafka message, can not be {@code null}.
     * @return status of the result of the consuming, can not be {@code null}.
     * @see Status
     */
    @Nonnull
    Status accept(@Nonnull ConsumerRecord<K, V> message);

    /**
     * Status of the result of the consuming.
     *
     * @author Alik
     */
    enum Status {

        /**
         * Processing was successful, the message was processed without errors.
         */
        OK,

        /**
         * In a result of processing the message was redirected to the DLQ.
         */
        ROUTED_TO_DLQ,

        /**
         * Processing failed, but the message was not redirected to the DLQ because
         * the number of attempts to resubmit to the DLQ exceeded the configured value.
         */
        DISCARDED_DLQ_MAX_ATTEMPTS_REACHED,

        /**
         * The message was forwarded to the redelivery queue.
         */
        ROUTED_TO_REDELIVERY_QUEUE,

        SCHEDULED_TO_REDELIVERY
    }
}
