package ru.joke.kdlq.impl.internal;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.KDLQLifecycleException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class KDLQProducerSessionTest {

    private static final String SESSION_ID = String.valueOf(312323);

    @Test
    public void testSessionLifecycleOnSingleUsage() {
        final var session = createSessionInUseWithChecks();
        makeChecksOnSessionClosing(session, mock(Runnable.class));
    }

    @Test
    public void testSessionLifecycleOnMultipleUsage() {
        final var session = createSessionInUseWithChecks();

        assertTrue(session.onUsage(), "Session must be available when isn't closed");

        final var callbackMock = mock(Runnable.class);
        assertFalse(session.close(0, callbackMock), "Session must not be closed");
        verify(callbackMock, never()).run();

        assertTrue(session.onUsage(), "Session must not be available when is closed");

        assertFalse(session.close(0, callbackMock), "Session must not be closed");
        verify(callbackMock, never()).run();

        makeChecksOnSessionClosing(session, callbackMock);
    }

    private KDLQProducerSession<String, byte[]> createSessionInUseWithChecks() {
        @SuppressWarnings("unchecked")
        final KafkaProducer<String, byte[]> producer = mock(KafkaProducer.class);
        final KDLQProducerSession<String, byte[]> session = new KDLQProducerSession<>(SESSION_ID, producer);

        assertEquals(SESSION_ID, session.sessionId(), "Session id must be equal");
        assertEquals(producer, session.producer(), "Producer must be equal");

        assertTrue(session.onUsage(), "Session must be available when isn't closed");

        return session;
    }

    private void makeChecksOnSessionClosing(final KDLQProducerSession<String, byte[]> session, final Runnable callbackMock) {

        assertTrue(session.close(0, callbackMock), "Session must be closed");
        verify(callbackMock, only()).run();

        makeClosedSessionChecks(session);
    }

    private void makeClosedSessionChecks(final KDLQProducerSession<String, byte[]> session) {
        assertFalse(session.onUsage(), "Session must not be available when is closed");

        final var callbackMock2 = mock(Runnable.class);
        assertThrows(KDLQLifecycleException.class, () -> session.close(0, callbackMock2), "Session must throw exception when is closed");
        verify(callbackMock2, never()).run();
    }
}
