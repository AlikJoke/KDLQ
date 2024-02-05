package ru.joke.kdlq.core.std;

import ru.joke.kdlq.core.KDLQConfiguration;
import ru.joke.kdlq.core.KDLQMessageLifecycleListener;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

public record DefaultKDLQConfiguration(
        @Nonnull Set<String> bootstrapServers,
        @Nonnull String deadLetterQueueName,
        @Nullable String redeliveryQueueName,
        @Nonnull Map<String, Object> producerProperties,
        int maxKills,
        int maxProcessingAttemptsCountBeforeKill,
        @Nonnull Set<KDLQMessageLifecycleListener> lifecycleListeners) implements KDLQConfiguration {
}
