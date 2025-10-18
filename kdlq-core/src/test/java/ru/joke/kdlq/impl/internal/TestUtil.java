package ru.joke.kdlq.impl.internal;

import ru.joke.kdlq.impl.internal.routers.DefaultKDLQMessageRouter;
import ru.joke.kdlq.internal.routers.KDLQMessageRouter;

import javax.annotation.Nonnull;

import static org.mockito.Mockito.mock;

public abstract class TestUtil {

    @Nonnull
    public static <K, V> KDLQMessageRouter<K, V> createSender() {
        @SuppressWarnings("unchecked")
        final KDLQMessageRouter<K, V> result = mock(DefaultKDLQMessageRouter.class);
        return result;
    }

    private TestUtil() {}
}
