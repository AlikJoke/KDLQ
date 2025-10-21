package ru.joke.kdlq.impl.internal;

import org.junit.jupiter.api.Test;
import ru.joke.kdlq.impl.internal.routers.KDLQProducerSession;
import ru.joke.kdlq.internal.routers.producers.KDLQProducersRegistry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class KDLQProducersRegistryTest {

    private final KDLQProducersRegistry registry = new KDLQProducersRegistry();

    @Test
    public void testWhenSessionNotRegistered() {
        final var producerId = "1";
        final var session = this.registry.get(producerId);

        assertNotNull(session, "Session wrapper must be not null");
        assertTrue(session.isEmpty(), "Session must not present");
    }

    @Test
    public void testWhenSessionRegistered() {
        final var producerId = "1";
        @SuppressWarnings("unchecked")
        final KDLQProducerSession<String, byte[]> mockSession1 = mock(KDLQProducerSession.class);
        @SuppressWarnings("unchecked")
        final KDLQProducerSession<String, byte[]> mockSession2 = mock(KDLQProducerSession.class);
        final var session1 = this.registry.registerIfNeed(producerId, () -> mockSession1);
        final var session2 = this.registry.registerIfNeed(producerId, () -> mockSession2);

        assertEquals(mockSession1, session1, "Sessions must be equal");
        assertEquals(session1, session2, "Sessions must be equal");

        final var sessionByIdWrapper = this.registry.get(producerId);
        assertTrue(sessionByIdWrapper.isPresent(), "Session must be present");
        assertEquals(session1, sessionByIdWrapper.get(), "Session must be equal");

        this.registry.unregister(producerId);
        final var sessionByIdWrapperAfterUnregistering = this.registry.get(producerId);
        assertTrue(sessionByIdWrapperAfterUnregistering.isEmpty(), "Session must not present");

        final var session2AfterUnregistering = this.registry.registerIfNeed(producerId, () -> mockSession2);
        assertEquals(mockSession2, session2AfterUnregistering, "Sessions must be equal");
    }
}
