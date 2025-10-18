package ru.joke.kdlq.spi;

import ru.joke.kdlq.KDLQConfiguration;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public interface KDLQRedeliveryStorage {

    void store(@Nonnull KDLQProducerRecord<?, ?> obj);

    default void deleteById(@Nonnull String objId) {
        deleteByIds(Set.of(objId));
    }

    void deleteByIds(@Nonnull Set<String> objectIds);

    @Nonnull
    List<KDLQProducerRecord<?, ?>> findAllReadyToRedelivery(
            @Nonnull Function<String, KDLQConfiguration> configurationFactory,
            @Nonnegative long redeliveryTimestamp
    );
}
