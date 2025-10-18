package ru.joke.kdlq.spi;

import org.apache.kafka.clients.producer.ProducerRecord;
import ru.joke.kdlq.KDLQConfiguration;

public interface KDLQProducerRecord<K, V> {

    String id();

    ProducerRecord<K, V> record();

    KDLQConfiguration configuration();

    long nextRedeliveryTimestamp();
}
