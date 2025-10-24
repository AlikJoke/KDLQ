package ru.joke.kdlq.internal.routers.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import ru.joke.kdlq.ImmutableKDLQConfiguration;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQException;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

class InternalKDLQMessageProducerTest {

    private static final String TEST_SESSION_ID = "test-session";
    private static final ProducerRecord<String, String> testRecord = new ProducerRecord<>("test", "1", "2");

    private String producerId;
    private KDLQProducersRegistry registry;
    private KDLQProducerSession<String, String> producerSession;
    private KDLQMessageProducer<String, String> messageProducer;
    private Producer<String, String> kafkaProducer;
    private Future<RecordMetadata> future;

    @BeforeEach
    void setUp() {
        this.future = mock(Future.class);
        this.registry = new InternalKDLQProducersRegistry();
        this.producerSession = mock(KDLQProducerSession.class);
        this.kafkaProducer = mock(Producer.class);

        when(this.producerSession.producer()).thenReturn(this.kafkaProducer);

        final Map<String, Object> producerProperties = Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
        );
        final var config =
                KDLQConfiguration.builder()
                        .withId(TEST_SESSION_ID)
                        .build(
                                Set.of("localhost:8080"),
                                producerProperties,
                                KDLQConfiguration.DLQ.builder().build("test-queue")
                        );
        this.producerId = config.producerId();
        when(this.producerSession.sessionId()).thenReturn(config.producerId());
        this.messageProducer = new InternalKDLQMessageProducer(this.registry, config);
    }

    @Test
    void testMessageProducerConstructorValidation() {
        assertThrows(
                KDLQException.class,
                () -> new InternalKDLQMessageProducer(
                        null,
                        mock(ImmutableKDLQConfiguration.class)
                )
        );

        assertThrows(
                KDLQException.class,
                () -> new InternalKDLQMessageProducer(
                        mock(InternalKDLQProducersRegistry.class),
                        null
                )
        );
    }

    @Test
    void testWhenSendSuccess() throws Exception {
        this.registry.registerIfNeed(this.producerId, () -> this.producerSession);
        when(this.producerSession.onUsage()).thenReturn(true);
        when(this.kafkaProducer.send(eq(testRecord), any(Callback.class)))
                .thenAnswer(invocation -> this.future);

        this.messageProducer.send(testRecord);

        final var session = this.registry.get(this.producerId);
        assertTrue(session.isPresent(), "Session must be registered");

        verify(this.producerSession).onUsage();

        final ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(this.kafkaProducer).send(recordCaptor.capture(), any(Callback.class));
        assertSame(testRecord, recordCaptor.getValue(), "Record must be same");
        verify(this.future).get(anyLong(), any(TimeUnit.class));
    }

    @Test
    void testWhenSendAndCallbackErrorThenThrowsException() {
        this.registry.registerIfNeed(this.producerId, () -> this.producerSession);
        when(this.producerSession.onUsage()).thenReturn(true);

        final var mockException = new RuntimeException("Kafka producer error");
        when(this.kafkaProducer.send(eq(testRecord), any(Callback.class)))
                .thenAnswer(invocation -> {
                    final Callback callback = invocation.getArgument(1);
                    callback.onCompletion(null, mockException);
                    return this.future;
                });

        final var exception = assertThrows(KDLQException.class, () -> this.messageProducer.send(testRecord));
        assertEquals(mockException, exception.getCause());

        verify(this.producerSession).onUsage();
        verify(this.kafkaProducer).send(eq(testRecord), any(Callback.class));
    }

    @Test
    void testWhenSendAndInterruptedExceptionThenThrowsInterruptedException() throws Exception {
        this.registry.registerIfNeed(this.producerId, () -> this.producerSession);

        when(this.producerSession.onUsage()).thenReturn(true);
        when(this.kafkaProducer.send(eq(testRecord), any(Callback.class)))
                .thenAnswer(invocation -> {
                    final Callback callback = invocation.getArgument(1);
                    callback.onCompletion(mock(RecordMetadata.class), null); // Simulate callback success
                    return future;
                });
        when(this.future.get(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException("Interrupted during send"));

        assertThrows(InterruptedException.class, () -> this.messageProducer.send(testRecord));
    }

    @Test
    void testWhenSendAndOnUsageReturnsFalseThenTrue() throws Exception {
        this.registry.registerIfNeed(this.producerId, () -> this.producerSession);

        when(this.producerSession.onUsage())
                .thenReturn(false)
                .thenReturn(true);

        when(this.kafkaProducer.send(eq(testRecord), any(Callback.class)))
                .thenAnswer(invocation -> {
                    final Callback callback = invocation.getArgument(1);
                    callback.onCompletion(mock(RecordMetadata.class), null);
                    return this.future;
                });

        this.messageProducer.send(testRecord);

        verify(this.producerSession, times(2)).onUsage();
        verify(this.kafkaProducer).send(eq(testRecord), any(Callback.class));
        verify(this.future).get(anyLong(), any(TimeUnit.class));
    }

    @Test
    void testWhenCloseAndSessionExistsAndClosedSuccessfully() {
        this.registry.registerIfNeed(this.producerId, () -> this.producerSession);
        when(this.producerSession.close(anyInt(), any(Runnable.class))).thenAnswer(invocation -> {
            Runnable unregisterCallback = invocation.getArgument(1);
            unregisterCallback.run();
            return true;
        });

        this.messageProducer.close();

        verify(this.producerSession).close(anyInt(), any(Runnable.class));

        final var producerSession = this.registry.get(this.producerId);
        assertTrue(producerSession.isEmpty(), "Session must be unregistered");
    }

    @Test
    void testWhenCloseAndSessionExistsAndCloseFails() {
        this.registry.registerIfNeed(this.producerId, () -> this.producerSession);
        when(this.producerSession.close(anyInt(), any(Runnable.class))).thenReturn(false);

        this.messageProducer.close();

        verify(this.producerSession).close(anyInt(), any(Runnable.class));

        final var producerSession = this.registry.get(this.producerId);
        assertTrue(producerSession.isPresent(), "Session must be registered");
    }

    @Test
    void testWhenCloseAndNoSessionExists() {
        this.messageProducer.close();
        verify(this.producerSession, never()).close(anyInt(), any(Runnable.class));
    }

    @Test
    void testWhenCloseMultipleTimesThenIdempotent() {
        this.registry.registerIfNeed(this.producerId, () -> this.producerSession);

        when(this.producerSession.close(anyInt(), any(Runnable.class))).thenAnswer(invocation -> {
            final Runnable unregisterCallback = invocation.getArgument(1);
            unregisterCallback.run();
            return true;
        });

        this.messageProducer.close();
        this.messageProducer.close();

        verify(this.producerSession).close(anyInt(), any(Runnable.class));

        final var producerSession = this.registry.get(this.producerId);
        assertTrue(producerSession.isEmpty(), "Session must be unregistered");
    }
}
