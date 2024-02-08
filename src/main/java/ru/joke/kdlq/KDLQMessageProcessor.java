package ru.joke.kdlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nonnull;

/**
 * A message handler that performs application processing logic. In the result of processing the message,
 * the implementation must return either one of the statuses, which will determine further actions on the
 * message, or generate an exception depending on what actions should be performed on the message.
 *
 * @param <K> type of the message key
 * @param <V> type of the message body value
 *
 * @author Alik
 * @see ProcessingStatus
 * @see KDLQMessageConsumer
 */
@FunctionalInterface
public interface KDLQMessageProcessor<K, V> {

    /**
     * Executes application logic to process the message.<br>
     * In the result should return one of the statuses {@link ProcessingStatus} that determines
     * further actions on the message or should throw an exception. If the exception is
     * {@link KDLQMessageMustBeRedeliveredException} then it is equivalent in action to returning the
     * {@link ProcessingStatus#MUST_BE_REDELIVERED} status.
     *
     * @param message message to processing, can not be {@code null}.
     * @return status that determines further actions on the message, can not be {@code null}.
     * @see ProcessingStatus
     */
    @Nonnull
    ProcessingStatus process(@Nonnull ConsumerRecord<K, V> message);

    /**
     * Status of the message processing.
     *
     * @author Alik
     */
    enum ProcessingStatus {

        /**
         * The message was successfully processed; no additional action is required.
         */
        OK,

        /**
         * The message must be redelivered for reprocessing.
         */
        MUST_BE_REDELIVERED,

        /**
         * An unexpected error occurred while processing the message.
         */
        ERROR
    }
}
