package ru.joke.kdlq.impl.internal;

import ru.joke.kdlq.KDLQConfiguration;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class KDLQMessageSenderFactory {

    private static final KDLQMessageSenderFactory instance = new KDLQMessageSenderFactory();

    @Nonnull
    public static KDLQMessageSenderFactory getInstance() {
        return instance;
    }

    @Nonnull
    public <K, V> KDLQMessageSender<K, V> create(@Nonnull String id, @Nonnull KDLQConfiguration configuration) {
        return new DefaultKDLQMessageSender<>(id, configuration);
    }
}
