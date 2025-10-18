package ru.joke.kdlq;

import ru.joke.kdlq.spi.KDLQDataConverter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

/**
 * Configuration of the KDLQ, which defines parameters for connecting to the Kafka broker to send erroneous
 * messages to the broker’s topic, as well as settings for message redelivery in case of errors.<br>
 *
 * The configuration allows you to define:
 * <ul>
 * <li>Broker servers where to send messages in case of errors.</li>
 * <li>Name of the DLQ queue (topic) where erroneous “dead” messages should be sent.</li>
 * <li>The name of the queue (topic) where messages should be sent in case of errors
 * that can be corrected by retrying processing.</li>
 * <li>Additional parameters of the Kafka message producer (for example, properties for authentication on the broker).</li>
 * <li>The number of maximum attempts to resend messages from DLQ to DLQ in case of repeated processing
 * errors before the message is considered “dead” and is ignored. If the value is negative,
 * the message is always re-sent back to the DLQ in case of errors.</li>
 * <li>The number of maximum attempts to deliver a message to the processing queue.
 * If exceeded, the message will be sent to the dead message queue (DLQ). If the value is negative,
 * then redelivery will occur indefinitely until the message is processed.</li>
 * <li>Optional message lifecycle listeners that allow code to be executed at
 * various stages of message processing.</li>
 * </ul>
 *
 * @author Alik
 * @see KDLQMessageLifecycleListener
 * @see KDLQMessageConsumer
 * @see KDLQMessageProcessor
 */
public interface KDLQConfiguration {

    /**
     * Returns the unique id of the configuration.
     *
     * @return can not be {@code null}.
     * @implSpec the identifier should depend only on the list of servers
     * and additional properties of the producer.
     */
    @Nonnull
    String id();

    /**
     * Returns the set of the Kafka servers.
     * @return servers, can not be {@code null} or empty.
     */
    @Nonnull
    Set<String> bootstrapServers();

    /**
     * Returns the additional properties of the Kafka producer.
     * @return additional properties, can not be {@code null}.
     */
    @Nonnull
    Map<String, Object> producerProperties();

    /**
     * Returns the message lifecycle listeners that allow code to be executed at
     * various stages of message processing.
     * @return can not be {@code null}.
     * @see KDLQMessageLifecycleListener
     */
    @Nonnull
    Set<KDLQMessageLifecycleListener> lifecycleListeners();

    /**
     * Returns whether optional information headers should be added to the message.
     * @return {@code true} if optional information headers should be added to the message, {@code false} otherwise.
     */
    boolean addOptionalInformationalHeaders();

    DLQ dlq();

    Redelivery redelivery();

    interface DLQ {

        /**
         * Returns the number of maximum attempts to resend messages from DLQ to DLQ in case of
         * repeated processing errors before the message is considered “dead” and is ignored.
         * @return the number of maximum attempts to resend messages from DLQ to DLQ.
         * If the value is negative, the message is always re-sent back to the DLQ in case of errors.
         */
        int maxKills();

        /**
         * Returns the DLQ name.
         * @return DLQ name, can not be {@code null}.
         */
        @Nonnull
        String deadLetterQueueName();
    }

    interface Redelivery {

        /**
         * Returns the queue (topic) to redelivery messages. If not set,
         * the original topic of the resent message will be used.
         * @return can be {@code null}.
         */
        @Nullable
        String redeliveryQueueName();

        /**
         * Returns the number of maximum attempts to deliver a message to the processing queue.
         * If exceeded, the message will be sent to the dead message queue (DLQ). If the value is negative,
         * then redelivery will occur indefinitely until the message is processed.
         * @return the number of maximum attempts to deliver a message
         */
        int maxRedeliveryAttemptsBeforeKill();

        double redeliveryDelayMultiplier();

        int redeliveryDelay();

        int maxRedeliveryDelay();

        <K> KDLQDataConverter<K> messageKeyConverter();

        <V> KDLQDataConverter<V> messageBodyConverter();
    }
}
