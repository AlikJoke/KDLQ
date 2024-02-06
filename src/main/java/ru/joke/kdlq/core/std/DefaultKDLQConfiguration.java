package ru.joke.kdlq.core.std;

import ru.joke.kdlq.core.KDLQConfiguration;
import ru.joke.kdlq.core.KDLQConfigurationException;
import ru.joke.kdlq.core.KDLQMessageLifecycleListener;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public record DefaultKDLQConfiguration(
        @Nonnull Set<String> bootstrapServers,
        @Nonnull String deadLetterQueueName,
        @Nullable String redeliveryQueueName,
        @Nonnull Map<String, Object> producerProperties,
        int maxKills,
        int maxRedeliveryAttemptsBeforeKill,
        @Nonnull Set<KDLQMessageLifecycleListener> lifecycleListeners) implements KDLQConfiguration {

    public DefaultKDLQConfiguration {

        if (bootstrapServers.isEmpty()) {
            throw new KDLQConfigurationException("Bootstrap servers must be not empty");
        }

        if (deadLetterQueueName.isBlank()) {
            throw new KDLQConfigurationException("DLQ name be not empty");
        }
    }

    public DefaultKDLQConfiguration(
            @Nonnull Set<String> bootstrapServers,
            @Nonnull String deadLetterQueueName,
            @Nonnull Map<String, Object> producerProperties,
            int maxKills,
            int maxRedeliveryAttemptsBeforeKill) {
        this(bootstrapServers, deadLetterQueueName, null, producerProperties, maxKills, maxRedeliveryAttemptsBeforeKill, Collections.emptySet());
    }

    public DefaultKDLQConfiguration(
            @Nonnull Set<String> bootstrapServers,
            @Nonnull String deadLetterQueueName,
            @Nullable String redeliveryQueueName,
            @Nonnull Map<String, Object> producerProperties,
            int maxKills,
            int maxRedeliveryAttemptsBeforeKill) {
        this(bootstrapServers, deadLetterQueueName, redeliveryQueueName, producerProperties, maxKills, maxRedeliveryAttemptsBeforeKill, Collections.emptySet());
    }
}
