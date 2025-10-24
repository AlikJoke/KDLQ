package ru.joke.kdlq.internal.routers.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.ImmutableKDLQConfiguration;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQException;
import ru.joke.kdlq.KDLQLifecycleException;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class InternalKDLQProducerSessionTest {

    private static final String SESSION_ID = String.valueOf(312323);

    @Test
    void testSessionConstructorValidation() {
        final var producer = mock(KafkaProducer.class);

        assertThrows(
                KDLQException.class,
                () -> new InternalKDLQProducerSession<>("", producer)
        );

        assertThrows(
                KDLQException.class,
                () -> new InternalKDLQProducerSession<>(SESSION_ID, null)
        );
    }

    @Test
    void testSessionLifecycleOnSingleUsage() {
        final var session = createSessionInUseWithChecks();
        makeChecksOnSessionClosing(session, mock(Runnable.class));
    }

    @Test
    void testSessionLifecycleOnMultipleUsage() {
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

    @Test
    void testSessionConstruction() {
        final Map<String, Object> producerProperties = Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
        );
        final var config =
                KDLQConfiguration.builder()
                                    .withId(SESSION_ID)
                                 .build(
                                         Set.of("localhost:8080"),
                                         producerProperties,
                                         KDLQConfiguration.DLQ.builder().build("test-queue")
                                 );
        final var session = new InternalKDLQProducerSession<>(config);

        assertEquals(config.producerId(), session.sessionId(), "Session id must be equal");
        assertNotNull(session.producer(), "Producer must be not null");
    }

    private KDLQProducerSession<String, byte[]> createSessionInUseWithChecks() {
        @SuppressWarnings("unchecked")
        final KafkaProducer<String, byte[]> producer = mock(KafkaProducer.class);
        final KDLQProducerSession<String, byte[]> session = new InternalKDLQProducerSession<>(SESSION_ID, producer);

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
