package ru.joke.kdlq.core;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.*;

public record KDLQConfiguration(
        @Nonnull Set<String> bootstrapServers,
        @Nonnull String queueName,
        @Nonnull Map<String, Object> producerProperties,
        @Nonnegative int maxKills,
        @Nonnegative int maxProcessingAttemptsCountBeforeKill) {
}
