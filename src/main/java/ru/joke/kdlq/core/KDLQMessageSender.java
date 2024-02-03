package ru.joke.kdlq.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nonnull;

public interface KDLQMessageSender {

    <K, V> void send(@Nonnull ConsumerRecord<K, V> message, @Nonnull KDLQConfiguration dlqConfiguration);
}
