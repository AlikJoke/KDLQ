package ru.joke.kdlq.internal.configs;

import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.spi.KDLQProducerRecord;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

final class InMemoryKDLQRedeliveryStorage implements KDLQRedeliveryStorage {

    private final Set<KDLQProducerRecord<?, ?>> storage = ConcurrentHashMap.newKeySet();

    @Override
    public void store(@Nonnull KDLQProducerRecord<?, ?> obj) {
        this.storage.add(obj);
    }

    @Override
    public void deleteByIds(@Nonnull Set<String> objectIds) {
        this.storage.removeIf(r -> objectIds.contains(r.id()));
    }

    @Nonnull
    @Override
    public List<KDLQProducerRecord<?, ?>> findAllReadyToRedelivery(
            @Nonnull final Function<String, KDLQConfiguration> configurationFactory,
            @Nonnegative final long redeliveryTimestamp
    ) {
        return this.storage.stream()
                            .filter(r -> r.nextRedeliveryTimestamp() <= redeliveryTimestamp)
                            .collect(Collectors.toList());
    }
}
