package ru.joke.kdlq.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nonnull;

public interface KDLQMessageConsumer<K, V> {

    @Nonnull
    Status accept(@Nonnull ConsumerRecord<K, V> message, @Nonnull KDLQMessageProcessor<K, V> messageProcessor);

    enum Status {

        OK,

        ERROR_DLQ_OK,

        ERROR_DLQ_MAX_ATTEMPTS_REACHED,

        WILL_BE_REDELIVERED
    }
}
