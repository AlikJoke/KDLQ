package ru.joke.kdlq.core;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

public interface KDLQConfiguration {

    @Nonnull
    String id();

    @Nonnull
    Set<String> bootstrapServers();

    @Nonnull
    String deadLetterQueueName();

    @Nullable
    String redeliveryQueueName();

    @Nonnull
    Map<String, Object> producerProperties();

    int maxKills();

    int maxRedeliveryAttemptsBeforeKill();

    @Nonnull
    Set<KDLQMessageLifecycleListener> lifecycleListeners();
}
