package ru.joke.kdlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nonnull;

/**
 * A message consumer that processes messages and resends messages when necessary or
 * sends them to the DLQ according to the specified KDLQ configuration.<br>
 * Example of usage:<br>
 * <pre>
 *     {@code
 *         final var configuration = buildConfiguration();
 *         final var consumer = KDLQ.registerConsumer("test", configuration, message -> {
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
 * @see KDLQMessageProcessor
 * @see KDLQConfiguration
 */
public interface KDLQMessageConsumer<K, V> extends AutoCloseable {

    /**
     * Accepts Kafka message and processes it.<br>
     * If processing is successful, it does not perform any further actions; a message is routed
     * to the DLQ if the processor throws a {@link KDLQMessageMustBeKilledException} or returns the status
     * {@link ru.joke.kdlq.KDLQMessageProcessor.ProcessingStatus#MUST_BE_KILLED}
     * (if the number of DLQ delivery attempts exceeds the configured limit, the message is dropped).
     * Otherwise, if the method returns {@link ru.joke.kdlq.KDLQMessageProcessor.ProcessingStatus#MUST_BE_REDELIVERED}
     * or throws any exception other than {@link KDLQMessageMustBeKilledException}, the message is sent
     * for redelivery to the redelivery queue (if the number of redelivery attempts exceeds the configured
     * limit, the message is then routed to the DLQ).
     *
     * @param message Kafka message; cannot be {@code null}.
     * @return status of the result of the consuming; cannot be {@code null}.
     * @see Status
     */
    @Nonnull
    Status accept(@Nonnull ConsumerRecord<K, V> message);

    /**
     * Returns unique id of this message consumer.
     *
     * @return id; cannot be {@code null} or empty.
     */
    @Nonnull
    String id();

    @Override
    void close();

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

        /**
         * Scheduled for redelivery (when delayed redelivery is configured).
         */
        SCHEDULED_TO_REDELIVERY
    }
}
