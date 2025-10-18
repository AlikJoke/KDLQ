package ru.joke.kdlq.internal.routers;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface KDLQMessageSender<K, V> extends AutoCloseable {

    void send(ProducerRecord<K, V> record) throws Exception;

    @Override
    void close();
}
