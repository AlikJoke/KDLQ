package ru.joke.kdlq.internal.redelivery;

import org.apache.kafka.clients.producer.ProducerRecord;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.spi.KDLQProducerRecord;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

public record DefaultKDLQProducerRecord<K, V>(
        @Nonnull String id,
        @Nonnull ProducerRecord<K, V> record,
        @Nonnull KDLQConfiguration configuration,
        @Nonnegative long nextRedeliveryTimestamp
) implements KDLQProducerRecord<K, V> {
}
