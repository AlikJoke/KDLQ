package ru.joke.kdlq.internal.routers;

import ru.joke.kdlq.KDLQConfiguration;

import javax.annotation.Nonnull;

public interface KDLQMessageSenderFactory {

    <K, V> KDLQMessageSender<K, V> create(@Nonnull KDLQConfiguration configuration);
}
