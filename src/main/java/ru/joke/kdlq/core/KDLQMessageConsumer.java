package ru.joke.kdlq.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nonnull;
import java.util.function.Predicate;

public interface KDLQMessageConsumer {

    @Nonnull
    <K, V> Status accept(@Nonnull ConsumerRecord<K, V> message, @Nonnull Predicate<ConsumerRecord<K, V>> action);

    enum Status {

        OK,

        ERROR_DLQ_OK,

        ERROR_RETRY
    }
}
