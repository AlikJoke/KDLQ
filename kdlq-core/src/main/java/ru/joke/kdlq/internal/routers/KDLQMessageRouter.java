package ru.joke.kdlq.internal.routers;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nonnull;
import java.io.Closeable;

public sealed interface KDLQMessageRouter<K, V> extends Closeable permits InternalKDLQMessageRouter {

    RoutingStatus routeToRedelivery(@Nonnull ConsumerRecord<K, V> originalMessage);

    RoutingStatus routeToDLQ(@Nonnull ConsumerRecord<K, V> originalMessage);

    @Override
    void close();

    enum RoutingStatus {

        ROUTED_TO_REDELIVERY_QUEUE,
        SCHEDULED_TO_REDELIVERY,
        ROUTED_TO_DLQ,
        DISCARDED
    }
}
