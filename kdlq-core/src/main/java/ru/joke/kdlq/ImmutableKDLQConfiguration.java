package ru.joke.kdlq;

import ru.joke.kdlq.internal.util.Args;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.util.*;

/**
 * Immutable implementation of the KDLQ configuration {@link KDLQConfiguration}.
 * For more convenient construction of the configuration object, use the builder {@link #builder()}.
 *
 * @author Alik
 * @see KDLQConfiguration
 * @see Builder
 */
@ThreadSafe
@Immutable
public final class ImmutableKDLQConfiguration implements KDLQConfiguration {

    private final Set<String> bootstrapServers;
    private final KDLQConfiguration.DLQ dlq;
    private final KDLQConfiguration.Redelivery redelivery;
    private final Map<String, Object> producerProperties;
    private final Set<KDLQMessageLifecycleListener> lifecycleListeners;
    private final boolean addOptionalInformationalHeaders;
    private final String id;

    private ImmutableKDLQConfiguration(
            @Nonnull String id,
            @Nonnull Set<String> bootstrapServers,
            @Nonnull Map<String, Object> producerProperties,
            @Nonnull KDLQConfiguration.DLQ dlq,
            @Nonnull KDLQConfiguration.Redelivery redelivery,
            @Nonnull Set<KDLQMessageLifecycleListener> lifecycleListeners,
            boolean addOptionalInformationalHeaders
    ) {

        Args.requireNotEmpty(bootstrapServers, () -> new KDLQConfigurationException("Bootstrap servers must be not empty"));
        Args.requireNotNull(producerProperties, () -> new KDLQConfigurationException("Kafka producer properties must be not null"));
        Args.requireNotEmpty(lifecycleListeners, () -> new KDLQConfigurationException("Lifecycle listeners must be not null"));
        Args.requireNotEmpty(id, () -> new KDLQConfigurationException("Configuration id must be not empty"));
        Args.requireNotEmpty(producerProperties, () -> new KDLQConfigurationException("Producer properties cannot be empty"));

        this.bootstrapServers = Set.copyOf(bootstrapServers);
        this.dlq = Args.requireNotNull(dlq, () -> new KDLQConfigurationException("DLQ be not empty"));
        this.redelivery = Args.requireNotNull(redelivery, () -> new KDLQConfigurationException("Redelivery be not empty"));
        this.producerProperties = Map.copyOf(producerProperties);
        this.lifecycleListeners = Set.copyOf(lifecycleListeners);
        this.addOptionalInformationalHeaders = addOptionalInformationalHeaders;
        this.id = id;
    }

    @Nonnull
    @Override
    public String id() {
        return this.id;
    }

    @Nonnull
    @Override
    public Set<String> bootstrapServers() {
        return this.bootstrapServers;
    }

    @Nonnull
    @Override
    public KDLQConfiguration.DLQ dlq() {
        return this.dlq;
    }

    @Nonnull
    @Override
    public KDLQConfiguration.Redelivery redelivery() {
        return this.redelivery;
    }

    @Nonnull
    @Override
    public Map<String, Object> producerProperties() {
        return this.producerProperties;
    }

    @Nonnull
    @Override
    public Set<KDLQMessageLifecycleListener> lifecycleListeners() {
        return this.lifecycleListeners;
    }

    @Override
    public boolean addOptionalInformationalHeaders() {
        return this.addOptionalInformationalHeaders;
    }

    @Override
    public String toString() {
        return "KDLQConfiguration{"
                + "id=" + id + '\''
                + ", bootstrapServers=" + bootstrapServers
                + ", dlq='" + dlq + '\''
                + ", redelivery='" + redelivery + '\''
                + ", lifecycleListeners=" + lifecycleListeners
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final var that = (ImmutableKDLQConfiguration) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    /**
     * Implementation of the {@link ru.joke.kdlq.KDLQConfiguration.Redelivery} configuration.
     *
     * @param redeliveryQueueName             the queue (topic) to redelivery messages; if not set,
     *                                        the original topic of the resent message will be used;
     *                                        can be {@code null}.
     * @param maxRedeliveryAttemptsBeforeKill the number of maximum attempts to deliver a message
     *                                        to the processing queue; if the value is negative redelivery
     *                                        will occur indefinitely until the message is processed
     * @param redeliveryDelayMultiplier       the multiplier for the delayed redelivery interval for
     *                                        subsequent redeliveries; cannot be {@code <= 0} if redelivery
     *                                        delay interval is set.
     * @param redeliveryDelay                 the initial delayed message redelivery interval in milliseconds;
     *                                        if the value is {@code 0}, redelivery will occur immediately.
     * @param maxRedeliveryDelay              the maximum delayed redelivery interval in milliseconds;
     *                                        cannot be {@code < 0}.
     */
    @Immutable
    @ThreadSafe
    public record Redelivery(
            @Nullable String redeliveryQueueName,
            int maxRedeliveryAttemptsBeforeKill,
            double redeliveryDelayMultiplier,
            @Nonnegative int redeliveryDelay,
            @Nonnegative long maxRedeliveryDelay
    ) implements KDLQConfiguration.Redelivery {

        public Redelivery {
            Args.requireNonNegative(redeliveryDelay, () -> new KDLQConfigurationException("Redelivery delay must be non negative"));
            Args.requireNonNegative(maxRedeliveryDelay, () -> new KDLQConfigurationException("Max redelivery delay must be non negative"));

            if (redeliveryDelayMultiplier <= 0) {
                throw new KDLQConfigurationException("Redelivery delay multiplier must be positive");
            }
        }

        /**
         * Returns a builder instance for more convenient construction of the redelivery object.
         *
         * @return builder instance; cannot be {@code null}.
         * @see ImmutableKDLQConfiguration.Redelivery.Builder
         */
        @Nonnull
        public static Builder builder() {
            return new Builder();
        }

        /**
         * Builder for the {@link ru.joke.kdlq.KDLQConfiguration.Redelivery} configuration object.<br>
         * This class is not thread-safe.
         */
        @NotThreadSafe
        public static class Builder {

            private String redeliveryQueueName;
            private int maxRedeliveryAttemptsBeforeKill = 5;
            private double redeliveryDelayMultiplier = 1.5;
            private int redeliveryDelay;
            private int maxRedeliveryDelay;

            /**
             * Sets the queue (topic) to redelivery messages
             *
             * @param redeliveryQueueName redelivery queue name; can be {@code null}.
             * @return the current builder object for further construction; cannot be {@code null}.
             * @see KDLQConfiguration.Redelivery#redeliveryQueueName()
             */
            @Nonnull
            public Builder withRedeliveryQueueName(@Nonnull String redeliveryQueueName) {
                this.redeliveryQueueName = redeliveryQueueName;
                return this;
            }

            /**
             * Sets the number of maximum attempts to deliver a message to the processing queue.<br>
             * Default value is {@code 5}.
             *
             * @param maxRedeliveryAttemptsBeforeKill the number of maximum attempts to deliver a message.
             * @return the current builder object for further construction; cannot be {@code null}.
             * @see KDLQConfiguration.Redelivery#maxRedeliveryAttemptsBeforeKill()
             */
            @Nonnull
            public Builder withMaxRedeliveryAttemptsBeforeKill(int maxRedeliveryAttemptsBeforeKill) {
                this.maxRedeliveryAttemptsBeforeKill = maxRedeliveryAttemptsBeforeKill;
                return this;
            }

            /**
             * Sets the multiplier for the delayed redelivery interval for subsequent redeliveries.<br>
             * Default value is {@code 1.5}.
             *
             * @param redeliveryDelayMultiplier the multiplier for the delayed redelivery interval
             *                                  for subsequent redeliveries; cannot be {@code  <= 0}.
             * @return the current builder object for further construction; cannot be {@code null}.
             * @see KDLQConfiguration.Redelivery#redeliveryDelayMultiplier()
             */
            @Nonnull
            public Builder withRedeliveryDelayMultiplier(double redeliveryDelayMultiplier) {
                this.redeliveryDelayMultiplier = redeliveryDelayMultiplier;
                return this;
            }

            /**
             * Sets the initial delayed message redelivery interval in milliseconds.
             *
             * @param redeliveryDelay the initial delayed message redelivery interval; cannot be negative.
             * @return the current builder object for further construction; cannot be {@code null}.
             * @see KDLQConfiguration.Redelivery#redeliveryDelay()
             */
            @Nonnull
            public Builder withRedeliveryDelay(@Nonnegative int redeliveryDelay) {
                this.redeliveryDelay = redeliveryDelay;
                return this;
            }

            /**
             * Sets the maximum delayed redelivery interval in milliseconds.
             *
             * @param maxRedeliveryDelay the maximum delayed redelivery interval in milliseconds; cannot be negative.
             * @return the current builder object for further construction; cannot be {@code null}.
             * @see KDLQConfiguration.Redelivery#maxRedeliveryDelay()
             */
            @Nonnull
            public Builder withMaxRedeliveryDelay(@Nonnegative int maxRedeliveryDelay) {
                this.maxRedeliveryDelay = maxRedeliveryDelay;
                return this;
            }

            /**
             * Creates a redelivery configuration object with the specified parameters.<br>
             *
             * @return created redelivery configuration object; cannot be {@code null}.
             * @see KDLQConfiguration.Redelivery
             */
            @Nonnull
            public KDLQConfiguration.Redelivery build() {
                return new Redelivery(
                        this.redeliveryQueueName,
                        this.maxRedeliveryAttemptsBeforeKill,
                        this.redeliveryDelayMultiplier,
                        this.redeliveryDelay,
                        this.maxRedeliveryDelay
                );
            }
        }
    }

    /**
     * Implementation of the {@link ru.joke.kdlq.KDLQConfiguration.DLQ} configuration.
     * @param deadLetterQueueName dead letter queue name; cannot be {@code null}.
     * @param maxKills            the number of maximum attempts to resend messages from DLQ
     *                            to DLQ in case of repeated processing errors before the
     *                            message is considered “dead” and is ignored.
     */
    public record DLQ(
            @Nonnull String deadLetterQueueName,
            int maxKills
    ) implements KDLQConfiguration.DLQ {

        public DLQ {
            Args.requireNotEmpty(deadLetterQueueName, () -> new KDLQConfigurationException("DLQ name must be not empty"));
        }

        /**
         * Returns a builder instance for more convenient construction of the DLQ object.
         *
         * @return builder instance; cannot be {@code null}.
         * @see ImmutableKDLQConfiguration.DLQ.Builder
         */
        @Nonnull
        public static Builder builder() {
            return new Builder();
        }

        /**
         * Builder for the {@link ru.joke.kdlq.KDLQConfiguration.DLQ} configuration object.<br>
         * This class is not thread-safe.
         */
        @NotThreadSafe
        public static class Builder {

            private int maxKills = -1;

            /**
             * Sets the number of maximum attempts to resend messages from DLQ to DLQ in case
             * of repeated processing errors before the message is considered “dead” and is ignored.<br>
             * Default value is {@code -1}.
             *
             * @param maxKills the number of maximum attempts to resend messages from DLQ to DLQ;
             *                 if the value is negative, the message is always re-sent back
             *                 to the DLQ in case of errors.
             * @return the current builder object for further construction; cannot be {@code null}.
             * @see KDLQConfiguration.DLQ#maxKills()
             */
            @Nonnull
            public Builder withMaxKills(int maxKills) {
                this.maxKills = maxKills;
                return this;
            }

            /**
             * Creates a DLQ configuration object with the specified parameters.<br>
             *
             * @param deadLetterQueueName name of the dead letter queue; cannot be {@code null} or empty.
             * @return created DLQ configuration object; cannot be {@code null}.
             * @see KDLQConfiguration.DLQ
             * @see KDLQConfiguration.DLQ#deadLetterQueueName()
             */
            @Nonnull
            public KDLQConfiguration.DLQ build(@Nonnull String deadLetterQueueName) {
                return new DLQ(deadLetterQueueName, this.maxKills);
            }
        }
    }

    /**
     * Returns a builder instance for more convenient construction of the {@link KDLQConfiguration} object.
     *
     * @return builder instance; cannot be {@code null}.
     * @see ImmutableKDLQConfiguration.Builder
     */
    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for the {@link ru.joke.kdlq.KDLQConfiguration} configuration object.<br>
     * This class is not thread-safe.
     */
    @NotThreadSafe
    public static class Builder {

        private String id;
        private final Set<KDLQMessageLifecycleListener> lifecycleListeners = new HashSet<>();
        private boolean addOptionalInformationalHeaders;
        private KDLQConfiguration.DLQ dlq;
        private KDLQConfiguration.Redelivery redelivery;

        /**
         * Sets the unique id of the configuration.<br>
         * Default value is {@code java.util.UUID.randomUUID().toString()}.<br>
         * <b>If delayed redelivery is used, the identifier must be "persistent" and
         * preserved across system reboots. Otherwise, messages stored in the repository
         * cannot be re-sent if the configuration under which the message was originally
         * saved for redelivery is not found.</b>
         *
         * @param id id of the configuration; if {@code null} the default value will be applied.
         * @return the current builder object for further construction; cannot be {@code null}.
         * @see KDLQConfiguration#id()
         */
        @Nonnull
        public Builder withId(final String id) {
            this.id = id;
            return this;
        }

        /**
         * Sets the message lifecycle listener.
         *
         * @param listener the message lifecycle listener; cannot be {@code null}.
         * @return the current builder object for further construction; cannot be {@code null}.
         * @see KDLQConfiguration#lifecycleListeners()
         * @see KDLQMessageLifecycleListener
         */
        @Nonnull
        public Builder withLifecycleListener(@Nonnull KDLQMessageLifecycleListener listener) {
            this.lifecycleListeners.add(Objects.requireNonNull(listener, "listener"));
            return this;
        }

        /**
         * Sets the message lifecycle listeners.
         *
         * @param listeners the message lifecycle listeners; cannot be {@code null}.
         * @return the current builder object for further construction; cannot be {@code null}.
         * @see KDLQConfiguration#lifecycleListeners()
         * @see KDLQMessageLifecycleListener
         */
        @Nonnull
        public Builder withLifecycleListeners(@Nonnull Set<KDLQMessageLifecycleListener> listeners) {
            this.lifecycleListeners.addAll(Objects.requireNonNull(listeners, "listeners"));
            return this;
        }

        /**
         * Sets the flag whether optional information headers should be added to the message.
         *
         * @param addInformationalHeaders whether optional information headers should be added to the message.
         * @return the current builder object for further construction; cannot be {@code null}.
         * @see KDLQConfiguration#addOptionalInformationalHeaders()
         */
        public Builder addInformationalHeaders(boolean addInformationalHeaders) {
            this.addOptionalInformationalHeaders = addInformationalHeaders;
            return this;
        }

        /**
         * Sets the redelivery configuration
         *
         * @param redelivery redelivery configuration; can be {@code null}.
         * @return the current builder object for further construction; cannot be {@code null}.
         * @see KDLQConfiguration#redelivery()
         */
        public Builder withRedelivery(@Nullable KDLQConfiguration.Redelivery redelivery) {
            this.redelivery = redelivery;
            return this;
        }

        /**
         * Creates a configuration object with the specified parameters.<br>
         * Mandatory parameters are passed as arguments to this method; others are optional,
         * and if not explicitly set in the builder, default values will be used.
         *
         * @param bootstrapServers   bootstraps servers; cannot be {@code null} or empty.
         * @param producerProperties properties of the Kafka producer; cannot be {@code null}.
         * @param dlq                dlq configuration; cannot be {@code null}.
         * @return created configuration object; cannot be {@code null}.
         * @see KDLQConfiguration#dlq()
         * @see KDLQConfiguration#bootstrapServers()
         */
        @Nonnull
        public KDLQConfiguration build(
                @Nonnull final Set<String> bootstrapServers,
                @Nonnull final Map<String, Object> producerProperties,
                @Nonnull final KDLQConfiguration.DLQ dlq
        ) {
            final var redelivery =
                    this.redelivery == null
                            ? Redelivery.builder().build()
                            : this.redelivery;
            final var id = this.id == null ? UUID.randomUUID().toString() : this.id;

            return new ImmutableKDLQConfiguration(
                    id,
                    bootstrapServers,
                    producerProperties,
                    dlq,
                    redelivery,
                    this.lifecycleListeners,
                    this.addOptionalInformationalHeaders
            );
        }
    }
}
