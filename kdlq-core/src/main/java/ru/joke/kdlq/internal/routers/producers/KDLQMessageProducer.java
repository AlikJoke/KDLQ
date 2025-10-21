package ru.joke.kdlq.internal.routers.producers;

import org.apache.kafka.clients.producer.ProducerRecord;

public sealed interface KDLQMessageProducer<K, V> extends AutoCloseable permits InternalKDLQMessageProducer {

    void send(ProducerRecord<K, V> record) throws Exception;

    @Override
    void close();
}
