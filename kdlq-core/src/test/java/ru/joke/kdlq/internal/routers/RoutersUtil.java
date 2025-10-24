package ru.joke.kdlq.internal.routers;

import static org.mockito.Mockito.mock;

public abstract class RoutersUtil {

    public static <K, V> KDLQMessageRouter<K, V> createRouterMock() {
        return mock(InternalKDLQMessageRouter.class);
    }

    private RoutersUtil() {}
}
