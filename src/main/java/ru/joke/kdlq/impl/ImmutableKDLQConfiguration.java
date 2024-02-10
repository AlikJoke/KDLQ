package ru.joke.kdlq.impl;

import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQConfigurationException;
import ru.joke.kdlq.KDLQMessageLifecycleListener;

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
    private final String deadLetterQueueName;
    private final String redeliveryQueueName;
    private final Map<String, Object> producerProperties;
    private final int maxKills;
    private final int maxRedeliveryAttemptsBeforeKill;
    private final Set<KDLQMessageLifecycleListener> lifecycleListeners;
    private final String id;

    public ImmutableKDLQConfiguration(
            @Nonnull Set<String> bootstrapServers,
            @Nonnull String deadLetterQueueName,
            @Nullable String redeliveryQueueName,
            @Nonnull Map<String, Object> producerProperties,
            int maxKills,
            int maxRedeliveryAttemptsBeforeKill,
            @Nonnull Set<KDLQMessageLifecycleListener> lifecycleListeners) {

        if (bootstrapServers.isEmpty()) {
            throw new KDLQConfigurationException("Bootstrap servers must be not empty");
        }

        if (deadLetterQueueName.isBlank()) {
            throw new KDLQConfigurationException("DLQ name be not empty");
        }

        this.bootstrapServers = Set.copyOf(bootstrapServers);
        this.deadLetterQueueName = Objects.requireNonNull(deadLetterQueueName, "deadLetterQueueName");
        this.redeliveryQueueName = redeliveryQueueName;
        this.producerProperties = Map.copyOf(producerProperties);
        this.maxKills = maxKills;
        this.maxRedeliveryAttemptsBeforeKill = maxRedeliveryAttemptsBeforeKill;
        this.lifecycleListeners = Set.copyOf(lifecycleListeners);
        this.id = this.bootstrapServers.hashCode() + "_" + this.producerProperties.hashCode();
    }

    public ImmutableKDLQConfiguration(
            @Nonnull Set<String> bootstrapServers,
            @Nonnull String deadLetterQueueName,
            @Nonnull Map<String, Object> producerProperties,
            int maxKills,
            int maxRedeliveryAttemptsBeforeKill) {
        this(bootstrapServers, deadLetterQueueName, null, producerProperties, maxKills, maxRedeliveryAttemptsBeforeKill, Collections.emptySet());
    }

    public ImmutableKDLQConfiguration(
            @Nonnull Set<String> bootstrapServers,
            @Nonnull String deadLetterQueueName,
            @Nullable String redeliveryQueueName,
            @Nonnull Map<String, Object> producerProperties,
            int maxKills,
            int maxRedeliveryAttemptsBeforeKill) {
        this(bootstrapServers, deadLetterQueueName, redeliveryQueueName, producerProperties, maxKills, maxRedeliveryAttemptsBeforeKill, Collections.emptySet());
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
    public String deadLetterQueueName() {
        return this.deadLetterQueueName;
    }

    @Nullable
    @Override
    public String redeliveryQueueName() {
        return this.redeliveryQueueName;
    }

    @Nonnull
    @Override
    public Map<String, Object> producerProperties() {
        return this.producerProperties;
    }

    @Override
    public int maxKills() {
        return this.maxKills;
    }

    @Override
    public int maxRedeliveryAttemptsBeforeKill() {
        return this.maxRedeliveryAttemptsBeforeKill;
    }

    @Nonnull
    @Override
    public Set<KDLQMessageLifecycleListener> lifecycleListeners() {
        return this.lifecycleListeners;
    }

    @Override
    public String toString() {
        return "DefaultKDLQConfiguration{"
                + "id=" + id + '\''
                + ", bootstrapServers=" + bootstrapServers
                + ", deadLetterQueueName='" + deadLetterQueueName + '\''
                + ", redeliveryQueueName='" + redeliveryQueueName + '\''
                + ", maxKills=" + maxKills
                + ", maxRedeliveryAttemptsBeforeKill=" + maxRedeliveryAttemptsBeforeKill
                + ", lifecycleListeners=" + lifecycleListeners
                + '}';
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final Set<KDLQMessageLifecycleListener> lifecycleListeners = new HashSet<>();
        private final Map<String, Object> producerProperties = new HashMap<>();
        private String redeliveryQueueName;
        private int maxKills = -1;
        private int maxRedeliveryAttemptsBeforeKill = -1;

        @Nonnull
        public Builder withLifecycleListener(@Nonnull KDLQMessageLifecycleListener listener) {
            this.lifecycleListeners.add(listener);
            return this;
        }

        @Nonnull
        public Builder withLifecycleListeners(@Nonnull Set<KDLQMessageLifecycleListener> listeners) {
            this.lifecycleListeners.addAll(listeners);
            return this;
        }

        @Nonnull
        public Builder withMaxKills(int maxKills) {
            this.maxKills = maxKills;
            return this;
        }

        @Nonnull
        public Builder withMaxRedeliveryAttemptsBeforeKill(int maxRedeliveryAttemptsBeforeKill) {
            this.maxRedeliveryAttemptsBeforeKill = maxRedeliveryAttemptsBeforeKill;
            return this;
        }

        @Nonnull
        public Builder withProducerProperties(@Nonnull Map<String, Object> producerProperties) {
            this.producerProperties.putAll(producerProperties);
            return this;
        }

        @Nonnull
        public KDLQConfiguration build(@Nonnull Set<String> bootstrapServers, @Nonnull String deadLetterQueueName) {
            return new ImmutableKDLQConfiguration(
                    bootstrapServers,
                    deadLetterQueueName,
                    this.redeliveryQueueName,
                    this.producerProperties,
                    this.maxKills,
                    this.maxRedeliveryAttemptsBeforeKill,
                    this.lifecycleListeners
            );
        }
    }
}
