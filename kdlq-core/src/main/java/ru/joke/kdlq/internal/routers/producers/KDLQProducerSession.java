package ru.joke.kdlq.internal.routers.producers;

import org.apache.kafka.clients.producer.Producer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

public interface KDLQProducerSession<K, V> {
    
    boolean close(
            @Nonnegative int timeoutSeconds, 
            @Nonnull Runnable onCloseCallback
    );

    boolean onUsage();

    @Nonnull
    String sessionId();

    @Nonnull
    Producer<K, V> producer();
}
