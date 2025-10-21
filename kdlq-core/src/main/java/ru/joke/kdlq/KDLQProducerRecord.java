package ru.joke.kdlq;

import org.apache.kafka.clients.producer.ProducerRecord;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A KDLQ message that is scheduled for redelivery in the future. It includes the
 * information necessary for the planned resending of the message.
 *
 * @param <K> type of message key
 * @param <V> type of message body
 *
 * @author Alik
 * @see KDLQRedeliveryStorage
 */
public interface KDLQProducerRecord<K, V> {

    /**
     * Returns the id of the message.
     *
     * @return message id; cannot be {@code null} or empty.
     */
    @Nonnull
    String id();

    /**
     * Returns the Kafka message to redelivery.
     *
     * @return Kafka message to redelivery; cannot be {@code null}.
     * @see ProducerRecord
     */
    @Nonnull
    ProducerRecord<K, V> record();

    /**
     * Returns the configuration of the original message's consumer that needs to be redelivered.
     *
     * @return configuration of the consumer; can be {@code null}: in such case the message
     *         will not be redelivered and will remain in the store.
     * @see KDLQConfiguration
     */
    @Nullable
    KDLQConfiguration configuration();

    /**
     * Returns the time of the message's next scheduled redelivery.
     *
     * @return the time of the next redelivery; cannot be negative.
     */
    @Nonnegative
    long nextRedeliveryTimestamp();
}
