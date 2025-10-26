package ru.joke.kdlq.internal.routers.producers;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;

/**
 * Representation of a Kafka message producer within the KDLQ subsystem.
 *
 * @param <K> type of message key
 * @param <V> type of message body
 * @author Alik
 */
public interface KDLQMessageProducer<K, V> extends AutoCloseable {

    /**
     * Sends message to Kafka.
     *
     * @param record Kafka message; cannot be {@code null}.
     * @throws Exception if sending failed or producer was closed
     */
    void send(@Nonnull ProducerRecord<K, V> record) throws Exception;

    @Override
    void close();
}
