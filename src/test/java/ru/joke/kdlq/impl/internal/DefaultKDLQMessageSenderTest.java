package ru.joke.kdlq.impl.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQException;
import ru.joke.kdlq.KDLQMessageLifecycleListener;
import ru.joke.kdlq.impl.ImmutableKDLQConfiguration;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static ru.joke.kdlq.impl.internal.DefaultKDLQMessageSender.*;

public class DefaultKDLQMessageSenderTest {

    private static final String DLQ_NAME = "test1";
    private static final String REDELIVERY_QUEUE_NAME = "test2";

    @Test
    public void testWhenMessageRedeliveryFail() {
        makeSendingChecks(
                REDELIVERY_QUEUE_NAME,
                context -> verify(context.lifecycleListener, only()).onMessageRedeliveryError(eq(context.producerId), eq(context.originalMessage), eq(context.message), any(Exception.class)),
                (sender, message) -> assertThrows(KDLQException.class, () -> sender.redeliver(message), "Redelivery should fail on send to Kafka"),
                false,
                false
        );
    }

    @Test
    public void testWhenMessageRedeliverySuccess() {
        makeSendingChecks(
                REDELIVERY_QUEUE_NAME,
                context -> verify(context.lifecycleListener, only()).onMessageRedeliverySuccess(eq(context.producerId), eq(context.originalMessage), eq(context.message)),
                (sender, message) -> assertTrue(sender.redeliver(message), "Redelivery should be success"),
                true,
                false
        );
    }

    @Test
    public void testWhenMessageRedeliveryAttemptsReached() {
        makeSendingChecks(
                DLQ_NAME,
                context -> verify(context.lifecycleListener, only()).onMessageKillSuccess(eq(context.producerId), eq(context.originalMessage), eq(context.message)),
                (sender, message) -> assertFalse(sender.redeliver(message), "Message should be sent to dlq during redelivery"),
                true,
                true
        );
    }

    @Test
    public void testWhenMessageSendingToDLQFail() {
        makeSendingToDLQChecks(
                context -> verify(context.lifecycleListener, only()).onMessageKillError(eq(context.producerId), eq(context.originalMessage), eq(context.message), any(Exception.class)),
                (sender, message) -> assertThrows(KDLQException.class, () -> sender.sendToDLQ(message), "Sending to DLQ should fail on send to Kafka"),
                false);
    }

    @Test
    public void testWhenMessageSendingToDLQSuccess() {
        makeSendingToDLQChecks(
                context -> verify(context.lifecycleListener, only()).onMessageKillSuccess(eq(context.producerId), eq(context.originalMessage), eq(context.message)),
                (sender, message) -> assertTrue(sender.sendToDLQ(message), "Sending to DLQ should be success"),
                true);
    }

    @Test
    public void testWhenMessageSendingToDLQAttemptsReached() {
        final var messageKey = "1";
        final var messageBody = "v".getBytes(StandardCharsets.UTF_8);
        final var processorId = UUID.randomUUID().toString();
        final var headersService = new KDLQHeadersService();

        final var expectedCounterHeader = headersService.createIntHeader(MESSAGE_KILLS_HEADER, 2);

        @SuppressWarnings("unchecked")
        final KafkaProducer<Object, Object> producer = mock(KafkaProducer.class);

        final var lifecycleListener = mock(KDLQMessageLifecycleListener.class);
        final var config = createConfig(lifecycleListener, -1, 2);
        final var registry = prepareProducersRegistryMock(config.id(), producer);

        final ConsumerRecord<Object, Object> message = new ConsumerRecord<>(DLQ_NAME, 0, 0, messageKey, messageBody);
        message.headers().add(expectedCounterHeader.key(), expectedCounterHeader.value());

        final KDLQMessageSender<Object, Object> sender = new DefaultKDLQMessageSender<>(processorId, config, registry);
        try (sender) {
            assertFalse(sender.sendToDLQ(message), "Sending to DLQ must be skipped");

            verifyNoInteractions(producer);

            verify(lifecycleListener, only()).onMessageSkip(processorId, message);
            verifyNoMoreInteractions(lifecycleListener);
        }
    }

    private void makeSendingToDLQChecks(
            final Consumer<TestContext<Object, Object>> lifecycleListenerChecks,
            final BiConsumer<KDLQMessageSender<Object, Object>, ConsumerRecord<Object, Object>> sendingResultChecks,
            final boolean sentMustBeSuccess) {
        makeSendingChecks(DLQ_NAME, lifecycleListenerChecks, sendingResultChecks, sentMustBeSuccess, true);
    }

    private void makeSendingChecks(
            final String expectedQueueName,
            final Consumer<TestContext<Object, Object>> lifecycleListenerChecks,
            final BiConsumer<KDLQMessageSender<Object, Object>, ConsumerRecord<Object, Object>> sendingResultChecks,
            final boolean sentMustBeSuccess,
            final boolean messageShouldBeSentToDLQ) {

        final var messageKey = "1";
        final var messageBody = "v".getBytes(StandardCharsets.UTF_8);
        final var processorId = UUID.randomUUID().toString();
        final var headersService = new KDLQHeadersService();

        final var expectedCounterHeader = headersService.createIntHeader(
                messageShouldBeSentToDLQ ? MESSAGE_KILLS_HEADER : MESSAGE_REDELIVERY_ATTEMPTS_HEADER,
                1
        );

        @SuppressWarnings("unchecked")
        final KafkaProducer<Object, Object> producer = mock(KafkaProducer.class);

        if (sentMustBeSuccess) {
            final CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            future.complete(null);

            when(producer.send(any(), any())).thenReturn(future);
        }

        final var lifecycleListener = mock(KDLQMessageLifecycleListener.class);
        final var config = createConfig(lifecycleListener, messageShouldBeSentToDLQ ? 0 : -1, messageShouldBeSentToDLQ ? -1 : 0);
        final var registry = prepareProducersRegistryMock(config.id(), producer);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<ProducerRecord<Object, Object>> messageCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        final ConsumerRecord<Object, Object> message = new ConsumerRecord<>(DLQ_NAME, 0, 0, messageKey, messageBody);

        final KDLQMessageSender<Object, Object> sender = new DefaultKDLQMessageSender<>(processorId, config, registry);
        try (sender) {
            sendingResultChecks.accept(sender, message);

            verify(producer, only()).send(messageCaptor.capture(), any());
            verifyNoMoreInteractions(producer);

            final ProducerRecord<Object, Object> messageToSend = messageCaptor.getValue();

            assertEquals(messageKey, messageToSend.key(), "Key must be equal");
            assertEquals(messageBody, messageToSend.value(), "Value must be equal");
            assertEquals(expectedQueueName, messageToSend.topic(), "Redelivery queue must be equal");

            final var headers = messageToSend.headers();
            assertNotNull(headers, "Headers must be not null");
            assertArrayEquals(processorId.getBytes(StandardCharsets.UTF_8), headers.lastHeader(MESSAGE_PRC_MARKER_HEADER).value(), "Processor id must be equal");

            final var counterHeader = headers.lastHeader(messageShouldBeSentToDLQ ? MESSAGE_KILLS_HEADER : MESSAGE_REDELIVERY_ATTEMPTS_HEADER);
            assertArrayEquals(expectedCounterHeader.value(), counterHeader.value(), "Redelivery counter must be equal");

            lifecycleListenerChecks.accept(new TestContext<>(processorId, message, messageToSend, lifecycleListener));
            verifyNoMoreInteractions(lifecycleListener);
        }
    }

    private KDLQProducersRegistry prepareProducersRegistryMock(final String producerId, final KafkaProducer<Object, Object> producer) {

        final var registry = mock(KDLQProducersRegistry.class);
        @SuppressWarnings("unchecked")
        final KDLQProducerSession<Object, Object> producerSession = mock(KDLQProducerSession.class);

        when(producerSession.producer()).thenReturn(producer);
        when(producerSession.onUsage()).thenReturn(true);

        when(registry.registerIfNeed(eq(producerId), any())).thenReturn(producerSession);

        return registry;
    }

    private KDLQConfiguration createConfig(
            final KDLQMessageLifecycleListener listener,
            final int maxRedeliveryAttempts,
            final int maxKills) {
        return ImmutableKDLQConfiguration
                    .builder()
                        .withRedeliveryQueueName(REDELIVERY_QUEUE_NAME)
                        .withLifecycleListener(listener)
                        .withMaxKills(maxKills)
                        .withMaxRedeliveryAttemptsBeforeKill(maxRedeliveryAttempts)
                    .build(Set.of("srv:01"), DLQ_NAME);
    }

    private record TestContext<K, V>(
            String producerId,
            ConsumerRecord<K, V> originalMessage,
            ProducerRecord<K, V> message,
            KDLQMessageLifecycleListener lifecycleListener) {
    }
}
