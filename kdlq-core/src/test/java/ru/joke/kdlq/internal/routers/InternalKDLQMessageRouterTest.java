package ru.joke.kdlq.internal.routers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import ru.joke.kdlq.*;
import ru.joke.kdlq.internal.routers.headers.KDLQHeadersService;
import ru.joke.kdlq.internal.routers.producers.InternalKDLQMessageProducerFactory;
import ru.joke.kdlq.internal.routers.producers.InternalKDLQProducersRegistry;
import ru.joke.kdlq.internal.routers.producers.KDLQProducerSession;
import ru.joke.kdlq.internal.routers.producers.KDLQProducersRegistry;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static ru.joke.kdlq.internal.routers.headers.KDLQHeaders.*;

class InternalKDLQMessageRouterTest {

    private static final String DLQ_NAME = "test1";
    private static final String REDELIVERY_QUEUE_NAME = "test2";

    @Test
    void testWhenMessageRedeliveryFail() {
        makeSendingChecks(
                REDELIVERY_QUEUE_NAME,
                context -> verify(context.lifecycleListener, only()).onMessageRedeliveryError(eq(context.producerId), eq(Optional.of(context.originalMessage)), eq(context.message), any(Exception.class)),
                (sender, message) -> assertThrows(KDLQException.class, () -> sender.routeToRedelivery(message), "Redelivery should fail on send to Kafka"),
                false,
                false
        );
    }

    @Test
    public void testWhenMessageRedeliverySuccess() {
        makeSendingChecks(
                REDELIVERY_QUEUE_NAME,
                context -> verify(context.lifecycleListener, only()).onMessageRedeliverySuccess(eq(context.producerId), eq(Optional.of(context.originalMessage)), eq(context.message)),
                (sender, message) -> assertEquals(KDLQMessageRouter.RoutingStatus.ROUTED_TO_REDELIVERY_QUEUE, sender.routeToRedelivery(message), "Redelivery should be success"),
                true,
                false
        );
    }

    @Test
    public void testWhenMessageRedeliveryToStorageSuccess() {
        makeRoutingToStorageChecks(
                REDELIVERY_QUEUE_NAME,
                true
        );
    }

    @Test
    public void testWhenMessageRedeliveryToStorageFail() {
        makeRoutingToStorageChecks(
                REDELIVERY_QUEUE_NAME,
                false
        );
    }

    @Test
    public void testWhenMessageRedeliveryAttemptsReached() {
        makeSendingChecks(
                DLQ_NAME,
                context -> verify(context.lifecycleListener, only()).onMessageKillSuccess(eq(context.producerId), eq(context.originalMessage), eq(context.message)),
                (sender, message) -> assertEquals(KDLQMessageRouter.RoutingStatus.ROUTED_TO_DLQ, sender.routeToRedelivery(message), "Message should be sent to dlq during redelivery"),
                true,
                true
        );
    }

    @Test
    public void testWhenMessageSendingToDLQFail() {
        makeSendingToDLQChecks(
                context -> verify(context.lifecycleListener, only()).onMessageKillError(eq(context.producerId), eq(context.originalMessage), eq(context.message), any(Exception.class)),
                (sender, message) -> assertThrows(KDLQException.class, () -> sender.routeToDLQ(message), "Sending to DLQ should fail on send to Kafka"),
                false);
    }

    @Test
    public void testWhenMessageSendingToDLQSuccess() {
        makeSendingToDLQChecks(
                context -> verify(context.lifecycleListener, only()).onMessageKillSuccess(eq(context.producerId), eq(context.originalMessage), eq(context.message)),
                (sender, message) -> assertEquals(KDLQMessageRouter.RoutingStatus.ROUTED_TO_DLQ, sender.routeToDLQ(message), "Sending to DLQ should be success"),
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
        final var config = createConfig(lifecycleListener, -1, 2, 0);
        final var registry = prepareProducersRegistryMock(config.producerId(), producer);

        final ConsumerRecord<Object, Object> message = new ConsumerRecord<>(DLQ_NAME, 0, 0, messageKey, messageBody);
        message.headers().add(expectedCounterHeader.key(), expectedCounterHeader.value());

        final var senderFactory = new InternalKDLQMessageProducerFactory(registry);
        final KDLQMessageRouter<Object, Object> sender = new InternalKDLQMessageRouter<>(
                processorId,
                config,
                senderFactory.create(config),
                () -> null,
                new KDLQHeadersService()
        );
        try (sender) {
            final var status = sender.routeToDLQ(message);
            assertEquals(KDLQMessageRouter.RoutingStatus.DISCARDED, status, "Sending to DLQ must be skipped");

            verifyNoInteractions(producer);

            verify(lifecycleListener, only()).onMessageDiscard(processorId, message);
            verifyNoMoreInteractions(lifecycleListener);
        }
    }

    private void makeSendingToDLQChecks(
            final Consumer<TestContext<Object, Object>> lifecycleListenerChecks,
            final BiConsumer<KDLQMessageRouter<Object, Object>, ConsumerRecord<Object, Object>> sendingResultChecks,
            final boolean sentMustBeSuccess) {
        makeSendingChecks(DLQ_NAME, lifecycleListenerChecks, sendingResultChecks, sentMustBeSuccess, true);
    }

    private void makeSendingChecks(
            final String expectedQueueName,
            final Consumer<TestContext<Object, Object>> lifecycleListenerChecks,
            final BiConsumer<KDLQMessageRouter<Object, Object>, ConsumerRecord<Object, Object>> sendingResultChecks,
            final boolean sentMustBeSuccess,
            final boolean messageShouldBeSentToDLQ
    ) {

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
        final var config = createConfig(lifecycleListener, messageShouldBeSentToDLQ ? 0 : -1, messageShouldBeSentToDLQ ? -1 : 0, 0);
        final var registry = prepareProducersRegistryMock(config.producerId(), producer);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<ProducerRecord<Object, Object>> messageCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        final ConsumerRecord<Object, Object> message = new ConsumerRecord<>(DLQ_NAME, 0, 0, messageKey, messageBody);

        final var senderFactory = new InternalKDLQMessageProducerFactory(registry);
        final KDLQMessageRouter<Object, Object> sender = new InternalKDLQMessageRouter<>(
                processorId,
                config,
                senderFactory.create(config),
                () -> null,
                new KDLQHeadersService()
        );

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

            final var offsetHeader = headers.lastHeader(MESSAGE_OFFSET_HEADER);
            final var headerFromOffset = headersService.createLongHeader(MESSAGE_OFFSET_HEADER, message.offset());
            assertArrayEquals(headerFromOffset.value(), offsetHeader.value(), "Offset must be equal");

            final var partitionHeader = headers.lastHeader(MESSAGE_PARTITION_HEADER);
            final var headerFromPartition = headersService.createIntHeader(MESSAGE_PARTITION_HEADER, message.partition());
            assertArrayEquals(headerFromPartition.value(), partitionHeader.value(), "Partition must be equal");

            final var timestampHeader = headers.lastHeader(MESSAGE_TIMESTAMP_HEADER);
            final var headerFromTimestamp = headersService.createLongHeader(MESSAGE_TIMESTAMP_HEADER, message.timestamp());
            assertArrayEquals(headerFromTimestamp.value(), timestampHeader.value(), "Timestamp must be equal");

            lifecycleListenerChecks.accept(new TestContext<>(processorId, message, messageToSend, lifecycleListener));
            verifyNoMoreInteractions(lifecycleListener);
        }
    }

    private void makeRoutingToStorageChecks(final String expectedQueueName, final boolean success) {

        final var messageKey = "1";
        final var messageBody = "v".getBytes(StandardCharsets.UTF_8);
        final var processorId = UUID.randomUUID().toString();
        final var headersService = new KDLQHeadersService();

        final var expectedCounterHeader = headersService.createIntHeader(
                MESSAGE_REDELIVERY_ATTEMPTS_HEADER,
                1
        );

        @SuppressWarnings("unchecked")
        final KafkaProducer<Object, Object> producer = mock(KafkaProducer.class);

        final var lifecycleListener = mock(KDLQMessageLifecycleListener.class);
        final var config = createConfig(lifecycleListener, -1, 0, 10);
        final var registry = prepareProducersRegistryMock(config.producerId(), producer);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<KDLQProducerRecord<byte[], byte[]>> messageCaptor = ArgumentCaptor.forClass(KDLQProducerRecord.class);
        final ConsumerRecord<Object, Object> message = new ConsumerRecord<>(REDELIVERY_QUEUE_NAME, 0, 0, messageKey, messageBody);

        final var senderFactory = new InternalKDLQMessageProducerFactory(registry);
        final var storage = mock(KDLQRedeliveryStorage.class);
        if (!success) {
            doThrow(RuntimeException.class).when(storage).store(any());
        }

        final KDLQMessageRouter<Object, Object> sender = new InternalKDLQMessageRouter<>(
                processorId,
                config,
                senderFactory.create(config),
                () -> storage,
                new KDLQHeadersService()
        );

        try (sender) {

            if (success) {
                final var status = sender.routeToRedelivery(message);
                assertEquals(KDLQMessageRouter.RoutingStatus.SCHEDULED_TO_REDELIVERY, status, "Message must be routed to redelivery storage");
            } else {
                assertThrows(KDLQException.class, () -> sender.routeToRedelivery(message));
            }

            verify(storage, only()).store(messageCaptor.capture());
            verifyNoInteractions(producer);
            verifyNoMoreInteractions(producer);

            final KDLQProducerRecord<byte[], byte[]> recordToStore = messageCaptor.getValue();
            final ProducerRecord<byte[], byte[]> messageToStore = recordToStore.record();

            assertArrayEquals(messageKey.getBytes(StandardCharsets.UTF_8), messageToStore.key(), "Key must be equal");
            assertEquals(messageBody, messageToStore.value(), "Value must be equal");
            assertEquals(expectedQueueName, messageToStore.topic(), "Redelivery queue must be equal");

            final var headers = messageToStore.headers();
            assertNotNull(headers, "Headers must be not null");
            assertArrayEquals(processorId.getBytes(StandardCharsets.UTF_8), headers.lastHeader(MESSAGE_PRC_MARKER_HEADER).value(), "Processor id must be equal");

            final var counterHeader = headers.lastHeader(MESSAGE_REDELIVERY_ATTEMPTS_HEADER);
            assertArrayEquals(expectedCounterHeader.value(), counterHeader.value(), "Redelivery counter must be equal");

            final var offsetHeader = headers.lastHeader(MESSAGE_OFFSET_HEADER);
            final var headerFromOffset = headersService.createLongHeader(MESSAGE_OFFSET_HEADER, message.offset());
            assertArrayEquals(headerFromOffset.value(), offsetHeader.value(), "Offset must be equal");

            final var partitionHeader = headers.lastHeader(MESSAGE_PARTITION_HEADER);
            final var headerFromPartition = headersService.createIntHeader(MESSAGE_PARTITION_HEADER, message.partition());
            assertArrayEquals(headerFromPartition.value(), partitionHeader.value(), "Partition must be equal");

            final var timestampHeader = headers.lastHeader(MESSAGE_TIMESTAMP_HEADER);
            final var headerFromTimestamp = headersService.createLongHeader(MESSAGE_TIMESTAMP_HEADER, message.timestamp());
            assertArrayEquals(headerFromTimestamp.value(), timestampHeader.value(), "Timestamp must be equal");

            if (success) {
                verify(lifecycleListener).onDeferredMessageRedeliverySchedulingSuccess(
                        eq(processorId),
                        eq(message),
                        assertArg(m -> makeMessageChecks(messageKey, messageBody, expectedQueueName, m))
                );
            } else {
                verify(lifecycleListener).onDeferredMessageRedeliverySchedulingError(
                        eq(processorId),
                        eq(message),
                        assertArg(m -> makeMessageChecks(messageKey, messageBody, expectedQueueName, m)),
                        assertArg(ex -> assertEquals(RuntimeException.class, ex.getClass()))
                );
            }
            verifyNoMoreInteractions(lifecycleListener);
        }
    }

    private void makeMessageChecks(
            final String expectedKey,
            final byte[] expectedBody,
            final String expectedQueueName,
            final ProducerRecord<Object, Object> actualMessage
    ) {
        assertEquals(expectedKey, actualMessage.key(), "Key must be equal");
        assertEquals(expectedBody, actualMessage.value(), "Value must be equal");
        assertEquals(expectedQueueName, actualMessage.topic(), "Redelivery queue must be equal");
    }

    private KDLQProducersRegistry prepareProducersRegistryMock(final String producerId, final KafkaProducer<Object, Object> producer) {

        final var registry = new InternalKDLQProducersRegistry();
        @SuppressWarnings("unchecked")
        final KDLQProducerSession<Object, Object> producerSession = mock(KDLQProducerSession.class);

        when(producerSession.producer()).thenReturn(producer);
        when(producerSession.onUsage()).thenReturn(true);

        registry.registerIfNeed(producerId, () -> producerSession);

        return registry;
    }

    private KDLQConfiguration createConfig(
            final KDLQMessageLifecycleListener listener,
            final int maxRedeliveryAttempts,
            final int maxKills,
            final int redeliveryDelay
    ) {
        final var redeliveryConfig =
                KDLQConfiguration.Redelivery.builder()
                                                .withRedeliveryQueueName(REDELIVERY_QUEUE_NAME)
                                                .withRedeliveryDelay(redeliveryDelay)
                                                .withMaxRedeliveryAttemptsBeforeKill(maxRedeliveryAttempts)
                                            .build();
        final var dlqConfig =
                KDLQConfiguration.DLQ.builder()
                                        .withMaxKills(maxKills)
                                     .build(DLQ_NAME);
        final Map<String, Object> producerProperties = Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
        );

        return ImmutableKDLQConfiguration
                    .builder()
                        .withRedelivery(redeliveryConfig)
                        .withLifecycleListener(listener)
                        .addInformationalHeaders(true)
                    .build(Set.of("srv:01"), producerProperties, dlqConfig);
    }

    private record TestContext<K, V>(
            String producerId,
            ConsumerRecord<K, V> originalMessage,
            ProducerRecord<K, V> message,
            KDLQMessageLifecycleListener lifecycleListener) {
    }
}
