package ru.joke.kdlq.impl.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nonnull;
import java.io.Closeable;

public sealed interface KDLQMessageSender<K, V> extends Closeable permits DefaultKDLQMessageSender {

    boolean redeliver(@Nonnull ConsumerRecord<K, V> originalMessage);

    boolean sendToDLQ(@Nonnull ConsumerRecord<K, V> originalMessage);

    @Override
    void close();
}
