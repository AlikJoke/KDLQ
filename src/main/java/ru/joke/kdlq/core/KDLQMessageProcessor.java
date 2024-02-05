package ru.joke.kdlq.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nonnull;

@FunctionalInterface
public interface KDLQMessageProcessor<K, V> {

    @Nonnull
    ProcessingStatus process(@Nonnull ConsumerRecord<K, V> message);

    enum ProcessingStatus {

        OK,

        MUST_BE_REDELIVERED,

        ERROR
    }
}
