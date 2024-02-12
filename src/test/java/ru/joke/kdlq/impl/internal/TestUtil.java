package ru.joke.kdlq.impl.internal;

import javax.annotation.Nonnull;

import static org.mockito.Mockito.mock;

public abstract class TestUtil {

    @Nonnull
    public static <K, V> KDLQMessageSender<K, V> createSender() {
        @SuppressWarnings("unchecked")
        final KDLQMessageSender<K, V> result = mock(DefaultKDLQMessageSender.class);
        return result;
    }

    private TestUtil() {}
}
