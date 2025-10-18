package ru.joke.kdlq;

import ru.joke.kdlq.internal.util.Args;
import ru.joke.kdlq.spi.KDLQDataConverter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.*;

/**
 * Default immutable implementation of the KDLQ configuration {@link KDLQConfiguration}.
 *
 * @author Alik
 * @see KDLQConfiguration
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

    public ImmutableKDLQConfiguration(
            @Nonnull Set<String> bootstrapServers,
            @Nonnull Map<String, Object> producerProperties,
            @Nonnull KDLQConfiguration.DLQ dlq,
            @Nonnull KDLQConfiguration.Redelivery redelivery,
            @Nonnull Set<KDLQMessageLifecycleListener> lifecycleListeners,
            boolean addOptionalInformationalHeaders
    ) {

        Args.requireNotEmpty(bootstrapServers, () -> new KDLQConfigurationException("Bootstrap servers must be not empty"));

        this.bootstrapServers = Set.copyOf(bootstrapServers);
        this.dlq = Args.requireNotNull(dlq, () -> new KDLQConfigurationException("DLQ be not empty"));
        this.redelivery = Args.requireNotNull(redelivery, () -> new KDLQConfigurationException("Redelivery be not empty"));
        this.producerProperties = Map.copyOf(producerProperties);
        this.lifecycleListeners = Set.copyOf(lifecycleListeners);
        this.addOptionalInformationalHeaders = addOptionalInformationalHeaders;
        this.id = this.bootstrapServers.hashCode() + "_" + this.producerProperties.hashCode();
    }

    public ImmutableKDLQConfiguration(
            @Nonnull Set<String> bootstrapServers,
            @Nonnull Map<String, Object> producerProperties,
            @Nonnull KDLQConfiguration.DLQ dlq,
            @Nonnull KDLQConfiguration.Redelivery redelivery
    ) {
        this(bootstrapServers, producerProperties, dlq, redelivery, Collections.emptySet(), false);
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
        return "DefaultKDLQConfiguration{"
                + "id=" + id + '\''
                + ", bootstrapServers=" + bootstrapServers
                + ", dlq='" + dlq + '\''
                + ", redelivery='" + redelivery + '\''
                + ", lifecycleListeners=" + lifecycleListeners
                + '}';
    }

    public record Redelivery(
            @Nullable String redeliveryQueueName,
            int maxRedeliveryAttemptsBeforeKill,
            double redeliveryDelayMultiplier,
            int redeliveryDelay,
            int maxRedeliveryDelay,
            @Nullable KDLQDataConverter<?> messageKeyConverter,
            @Nullable KDLQDataConverter<?> messageBodyConverter
    ) implements KDLQConfiguration.Redelivery {

        public Redelivery {
            if (redeliveryDelay > 0 && (messageKeyConverter == null || messageBodyConverter == null)) {
                throw new KDLQConfigurationException("Converters must be provided when redelivery delay is specified");
            }
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {

            private String redeliveryQueueName;
            private int maxRedeliveryAttemptsBeforeKill = -1;
            private double redeliveryDelayMultiplier;
            private int redeliveryDelay;
            private int maxRedeliveryDelay;
            private KDLQDataConverter<?> messageKeyConverter;
            private KDLQDataConverter<?> messageBodyConverter;

            @Nonnull
            public Builder withRedeliveryQueueName(@Nonnull String redeliveryQueueName) {
                this.redeliveryQueueName = redeliveryQueueName;
                return this;
            }

            @Nonnull
            public Builder withMaxRedeliveryAttemptsBeforeKill(int maxRedeliveryAttemptsBeforeKill) {
                this.maxRedeliveryAttemptsBeforeKill = maxRedeliveryAttemptsBeforeKill;
                return this;
            }

            @Nonnull
            public Builder withRedeliveryDelayMultiplier(double redeliveryDelayMultiplier) {
                this.redeliveryDelayMultiplier = redeliveryDelayMultiplier;
                return this;
            }

            @Nonnull
            public Builder withRedeliveryDelay(int redeliveryDelay) {
                this.redeliveryDelay = redeliveryDelay;
                return this;
            }

            @Nonnull
            public Builder withMaxRedeliveryDelay(int maxRedeliveryDelay) {
                this.maxRedeliveryDelay = maxRedeliveryDelay;
                return this;
            }

            @Nonnull
            public Builder withMessageKeyConverter(@Nullable KDLQDataConverter<?> messageKeyConverter) {
                this.messageKeyConverter = messageKeyConverter;
                return this;
            }

            @Nonnull
            public Builder withMessageBodyConverter(@Nullable KDLQDataConverter<?> messageBodyConverter) {
                this.messageBodyConverter = messageBodyConverter;
                return this;
            }

            public KDLQConfiguration.Redelivery build() {
                return new Redelivery(
                        this.redeliveryQueueName,
                        this.maxRedeliveryAttemptsBeforeKill,
                        this.redeliveryDelayMultiplier,
                        this.redeliveryDelay,
                        this.maxRedeliveryDelay,
                        this.messageKeyConverter,
                        this.messageBodyConverter
                );
            }
        }
    }

    public record DLQ(
            @Nonnull String deadLetterQueueName,
            int maxKills
    ) implements KDLQConfiguration.DLQ {

        public DLQ {
            Args.requireNotEmpty(deadLetterQueueName, () -> new KDLQConfigurationException("DLQ name must be not empty"));
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {

            private int maxKills = -1;

            @Nonnull
            public Builder withMaxKills(int maxKills) {
                this.maxKills = maxKills;
                return this;
            }

            public KDLQConfiguration.DLQ build(@Nonnull String deadLetterQueueName) {
                return new DLQ(deadLetterQueueName, this.maxKills);
            }
        }
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final Set<KDLQMessageLifecycleListener> lifecycleListeners = new HashSet<>();
        private final Map<String, Object> producerProperties = new HashMap<>();
        private boolean addOptionalInformationalHeaders;
        private KDLQConfiguration.DLQ dlq;
        private KDLQConfiguration.Redelivery redelivery;

        @Nonnull
        public Builder withLifecycleListener(@Nonnull KDLQMessageLifecycleListener listener) {
            this.lifecycleListeners.add(Objects.requireNonNull(listener, "listener"));
            return this;
        }

        @Nonnull
        public Builder withLifecycleListeners(@Nonnull Set<KDLQMessageLifecycleListener> listeners) {
            this.lifecycleListeners.addAll(Objects.requireNonNull(listeners, "listeners"));
            return this;
        }

        @Nonnull
        public Builder withProducerProperties(@Nonnull Map<String, Object> producerProperties) {
            this.producerProperties.putAll(Objects.requireNonNull(producerProperties, "producerProperties"));
            return this;
        }

        public Builder addInformationalHeaders(boolean addInformationalHeaders) {
            this.addOptionalInformationalHeaders = addInformationalHeaders;
            return this;
        }

        public Builder withRedelivery(KDLQConfiguration.Redelivery redelivery) {
            this.redelivery = redelivery;
            return this;
        }

        @Nonnull
        public KDLQConfiguration build(@Nonnull Set<String> bootstrapServers, @Nonnull DLQ dlq) {
            final var redelivery =
                    this.redelivery == null
                            ? Redelivery.builder().build()
                            : this.redelivery;
            return new ImmutableKDLQConfiguration(
                    bootstrapServers,
                    this.producerProperties,
                    dlq,
                    redelivery,
                    this.lifecycleListeners,
                    this.addOptionalInformationalHeaders
            );
        }
    }
}
