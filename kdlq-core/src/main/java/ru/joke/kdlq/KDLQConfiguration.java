package ru.joke.kdlq;

import ru.joke.kdlq.spi.KDLQDataConverter;

import javax.annotation.Nonnegative;
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
 * <li>The delayed redelivery interval (in milliseconds) for messages in case of errors.</li>
 * <li>The multiplier for the delayed delivery interval across a series of redeliveries.</li>
 * <li>The maximum delayed message redelivery interval (a ceiling for interval calculation,
 * considering the multiplier).</li>
 * <li>Kafka message key converter for storing messages awaiting redelivery.</li>
 * <li>Kafka message body converter for storing messages awaiting redelivery.</li>
 * <li>Optional message lifecycle listeners that allow code to be executed at
 * various stages of message processing.</li>
 * </ul>
 *
 * @author Alik
 * @see KDLQMessageLifecycleListener
 * @see KDLQMessageConsumer
 * @see KDLQMessageProcessor
 * @see KDLQDataConverter
 */
public sealed interface KDLQConfiguration permits ImmutableKDLQConfiguration {

    /**
     * Returns the unique id of the configuration.
     *
     * @return id; cannot be {@code null}.
     */
    @Nonnull
    String id();

    /**
     * Returns the unique producer session id calculated based on this configuration.
     * 
     * @return producer session id; cannot be {@code null}.
     */
    default String producerId() {
        return bootstrapServers().hashCode() + "_" + producerProperties().hashCode();
    }

    /**
     * Returns the set of the Kafka servers.
     *
     * @return servers; cannot be {@code null} or empty.
     */
    @Nonnull
    Set<String> bootstrapServers();

    /**
     * Returns the additional properties of the Kafka producer.
     *
     * @return additional properties; cannot be {@code null}.
     */
    @Nonnull
    Map<String, Object> producerProperties();

    /**
     * Returns the message lifecycle listeners that allow code to be executed at
     * various stages of message processing.
     *
     * @return cannot be {@code null}.
     * @see KDLQMessageLifecycleListener
     */
    @Nonnull
    Set<KDLQMessageLifecycleListener> lifecycleListeners();

    /**
     * Returns whether optional information headers should be added to the message.
     *
     * @return {@code true} if optional information headers should be added to the message, {@code false} otherwise.
     */
    boolean addOptionalInformationalHeaders();

    /**
     * Returns DLQ (Dead Letter Queue) settings.
     *
     * @return DLQ settings; cannot be {@code null}.
     * @see DLQ
     */
    @Nonnull
    DLQ dlq();

    /**
     * Returns message redelivery settings.
     *
     * @return redelivery settings; cannot be {@code null}.
     * @see Redelivery
     */
    @Nonnull
    Redelivery redelivery();

    /**
     * Returns a builder instance for more convenient construction of the configuration object.
     *
     * @return builder instance; cannot be {@code null}.
     * @see ImmutableKDLQConfiguration.Builder
     */
    @Nonnull
    static ImmutableKDLQConfiguration.Builder builder() {
        return new ImmutableKDLQConfiguration.Builder();
    }

    /**
     * Dead letter queue settings.
     */
    interface DLQ {

        /**
         * Returns the number of maximum attempts to resend messages from DLQ to DLQ in case of
         * repeated processing errors before the message is considered “dead” and is ignored.
         *
         * @return the number of maximum attempts to resend messages from DLQ to DLQ.
         * If the value is negative, the message is always re-sent back to the DLQ in case of errors.
         */
        int maxKills();

        /**
         * Returns the DLQ name.
         *
         * @return DLQ name; cannot be {@code null}.
         */
        @Nonnull
        String deadLetterQueueName();

        /**
         * Returns a builder instance for more convenient construction of the DLQ object.
         *
         * @return builder instance; cannot be {@code null}.
         * @see ImmutableKDLQConfiguration.DLQ.Builder
         */
        @Nonnull
        static ImmutableKDLQConfiguration.DLQ.Builder builder() {
            return new ImmutableKDLQConfiguration.DLQ.Builder();
        }
    }

    /**
     * Message redelivery settings.
     */
    interface Redelivery {

        /**
         * Returns the queue (topic) to redelivery messages. If not set,
         * the original topic of the resent message will be used.
         *
         * @return can be {@code null}.
         */
        @Nullable
        String redeliveryQueueName();

        /**
         * Returns the number of maximum attempts to deliver a message to the processing queue.
         * If exceeded, the message will be sent to the dead message queue (DLQ). If the value is negative,
         * then redelivery will occur indefinitely until the message is processed.
         *
         * @return the number of maximum attempts to deliver a message.
         */
        int maxRedeliveryAttemptsBeforeKill();

        /**
         * Returns the multiplier for the delayed redelivery interval for subsequent redeliveries.
         *
         * @return multiplier for the delayed redelivery interval;
         * cannot be {@code <= 0} if redelivery delay interval is set.
         */
        @Nonnegative
        double redeliveryDelayMultiplier();

        /**
         * Returns the initial delayed message redelivery interval in milliseconds.
         * If the value is {@code 0}, redelivery will occur immediately, bypassing the redelivery message storage.
         *
         * @return the initial delayed message redelivery interval; cannot be {@code < 0}.
         */
        @Nonnegative
        int redeliveryDelay();

        /**
         * Returns the maximum delayed redelivery interval in milliseconds.
         *
         * @return the maximum delayed redelivery interval; cannot be {@code < 0}.
         */
        @Nonnegative
        int maxRedeliveryDelay();

        /**
         * Returns Kafka message key converter for storing messages awaiting redelivery.
         *
         * @param <K> type of message key
         * @return message key converter; can be {@code null} if redelivery storage
         * is not used (redelivery delay must be {@code 0}).
         * @see KDLQDataConverter
         */
        @Nullable
        <K> KDLQDataConverter<K> messageKeyConverter();

        /**
         * Returns Kafka message body converter for storing messages awaiting redelivery.
         *
         * @param <V> type of message body
         * @return message body converter; can be {@code null} if redelivery storage
         * is not used (redelivery delay must be {@code 0}).
         * @see KDLQDataConverter
         */
        @Nullable
        <V> KDLQDataConverter<V> messageBodyConverter();

        /**
         * Returns a builder instance for more convenient construction of the redelivery object.
         *
         * @return builder instance; cannot be {@code null}.
         * @see ImmutableKDLQConfiguration.Redelivery.Builder
         */
        @Nonnull
        static ImmutableKDLQConfiguration.Redelivery.Builder builder() {
            return new ImmutableKDLQConfiguration.Redelivery.Builder();
        }
    }
}
