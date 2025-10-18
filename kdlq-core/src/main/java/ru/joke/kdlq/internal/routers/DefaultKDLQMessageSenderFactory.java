package ru.joke.kdlq.internal.routers;

import ru.joke.kdlq.KDLQConfiguration;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class DefaultKDLQMessageSenderFactory implements KDLQMessageSenderFactory {

    @Nonnull
    public <K, V> KDLQMessageSender<K, V> create(@Nonnull KDLQConfiguration configuration) {
        return new DefaultKDLQMessageSender<>(configuration);
    }
}
