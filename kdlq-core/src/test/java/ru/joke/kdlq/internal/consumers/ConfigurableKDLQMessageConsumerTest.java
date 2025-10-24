package ru.joke.kdlq.internal.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.*;
import ru.joke.kdlq.internal.routers.InternalKDLQMessageRouterFactory;
import ru.joke.kdlq.internal.routers.KDLQMessageRouter;
import ru.joke.kdlq.internal.routers.RoutersUtil;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class ConfigurableKDLQMessageConsumerTest {

    private static final String CONSUMER_ID = "test";

    @Test
    void testConsumerConstructorValidation() {
        final var config = mock(ImmutableKDLQConfiguration.class);
        final var processor = mock(KDLQMessageProcessor.class);
        final var messageRouter = RoutersUtil.createRouterMock();
        final var callback = mock(Consumer.class);

        assertThrows(
                KDLQException.class,
                () -> new ConfigurableKDLQMessageConsumer<>("", config, processor, messageRouter, callback)
        );

        assertThrows(
                KDLQException.class,
                () -> new ConfigurableKDLQMessageConsumer<>(CONSUMER_ID, null, processor, messageRouter, callback)
        );

        assertThrows(
                KDLQException.class,
                () -> new ConfigurableKDLQMessageConsumer<>(CONSUMER_ID, config, null, messageRouter, callback)
        );

        assertThrows(
                KDLQException.class,
                () -> new ConfigurableKDLQMessageConsumer<>(CONSUMER_ID, config, processor, null, callback)
        );

        assertThrows(
                KDLQException.class,
                () -> new ConfigurableKDLQMessageConsumer<>(CONSUMER_ID, config, processor, messageRouter, null)
        );
    }

    @Test
    void testWhenProcessingSuccess() {
        execute(
                this::makeSuccessChecks,
                m -> KDLQMessageProcessor.ProcessingStatus.OK
        );
    }

    @Test
    void testConsumerId() {
        execute(
                context -> assertEquals(CONSUMER_ID, context.consumer.id(), "Consumer id must be equal"),
                m -> KDLQMessageProcessor.ProcessingStatus.OK
        );
    }

    @Test
    void testWhenProcessingFailedThenSendToDLQShouldBeInvoked() {
        execute(
                context -> makeUnrecoverableErrorChecks(context, null),
                m -> KDLQMessageProcessor.ProcessingStatus.MUST_BE_KILLED
        );
        final var handlerError = new KDLQMessageMustBeKilledException("some cause");
        execute(
                context -> makeUnrecoverableErrorChecks(context, handlerError),
                m -> {
                    throw handlerError;
                }
        );
    }

    @Test
    void testWhenProcessingFinishedWithRedeliveryThenRedeliveryShouldBeInvoked() {
        execute(
                context -> makeRedeliveryErrorChecks(context, null),
                m -> KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED
        );
        final var handlerRedeliveryError = new KDLQMessageMustBeRedeliveredException("some cause");
        execute(
                context -> makeRedeliveryErrorChecks(context, handlerRedeliveryError),
                m -> {
                    throw handlerRedeliveryError;
                }
        );

        final var handlerUnexpectedError = new KDLQMessageMustBeRedeliveredException("some cause");
        execute(
                context -> makeRedeliveryErrorChecks(context, handlerUnexpectedError),
                m -> {
                    throw handlerUnexpectedError;
                }
        );
    }

    @Test
    void testWhenProcessingFinishedWithRedeliveryAndDelayedRedeliveryConfiguredThenRedeliverySchedulingShouldBeInvoked() {
        execute(
                context -> makeRedeliverySchedulingErrorChecks(context, null),
                m -> KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED
        );
        final var handlerError = new KDLQMessageMustBeRedeliveredException("some cause");
        execute(
                context -> makeRedeliverySchedulingErrorChecks(context, handlerError),
                m -> {
                    throw handlerError;
                }
        );
    }

    @Test
    void testWhenConsumerClosed() {
        execute(
                context -> {
                    context.consumer.close();
                    assertThrows(KDLQLifecycleException.class, () -> context.consumer.accept(context.message));
                },
                m -> KDLQMessageProcessor.ProcessingStatus.OK,
                true,
                mock(Consumer.class)
        );
    }

    @Test
    void testWhenCloseCallbackThrowsException() {
        final var callback = mock(Consumer.class);
        doThrow(RuntimeException.class).when(callback).accept(any());

        execute(
                context -> {},
                m -> KDLQMessageProcessor.ProcessingStatus.OK,
                false,
                mock(Consumer.class)
        );
    }

    private void makeUnrecoverableErrorChecks(final TestContext context, final RuntimeException expectedHandlerError) {
        when(context.sender.routeToDLQ(context.message)).thenReturn(KDLQMessageRouter.RoutingStatus.ROUTED_TO_DLQ);
        final var status1 = context.consumer.accept(context.message);

        when(context.sender.routeToDLQ(context.message)).thenReturn(KDLQMessageRouter.RoutingStatus.DISCARDED);
        final var status2 = context.consumer.accept(context.message);

        assertEquals(KDLQMessageConsumer.Status.ROUTED_TO_DLQ, status1, "Status must be equal");
        assertEquals(KDLQMessageConsumer.Status.DISCARDED_DLQ_MAX_ATTEMPTS_REACHED, status2, "Status must be equal");

        verify(context.sender, times(2)).routeToDLQ(context.message);
        verifyNoMoreInteractions(context.sender);
        verify(context.listener, times(2)).onMessageProcessing(CONSUMER_ID, context.message, KDLQMessageProcessor.ProcessingStatus.MUST_BE_KILLED, expectedHandlerError);
        verifyNoMoreInteractions(context.listener);
    }

    private void makeSuccessChecks(final TestContext context) {
        final var status1 = context.consumer.accept(context.message);
        final var status2 = context.consumer.accept(context.message);

        assertEquals(KDLQMessageConsumer.Status.OK, status1, "Status must be equal");
        assertEquals(KDLQMessageConsumer.Status.OK, status2, "Status must be equal");

        verifyNoInteractions(context.sender);
        verify(context.listener, times(2)).onMessageProcessing(CONSUMER_ID, context.message, KDLQMessageProcessor.ProcessingStatus.OK, null);
        verifyNoMoreInteractions(context.listener);
    }

    private void makeRedeliveryErrorChecks(
            final TestContext context,
            final KDLQMessageMustBeRedeliveredException expectedException
    ) {
        when(context.sender.routeToRedelivery(context.message)).thenReturn(KDLQMessageRouter.RoutingStatus.ROUTED_TO_REDELIVERY_QUEUE);
        final var status1 = context.consumer.accept(context.message);
        assertEquals(KDLQMessageConsumer.Status.ROUTED_TO_REDELIVERY_QUEUE, status1, "Status must be equal");

        when(context.sender.routeToRedelivery(context.message)).thenReturn(KDLQMessageRouter.RoutingStatus.ROUTED_TO_DLQ);
        final var status2 = context.consumer.accept(context.message);
        assertEquals(KDLQMessageConsumer.Status.ROUTED_TO_DLQ, status2, "Status must be equal");

        verify(context.sender, times(2)).routeToRedelivery(context.message);
        verifyNoMoreInteractions(context.sender);
        verify(context.listener, times(2)).onMessageProcessing(CONSUMER_ID, context.message, KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED, expectedException);
        verifyNoMoreInteractions(context.listener);
    }

    private void makeRedeliverySchedulingErrorChecks(
            final TestContext context,
            final KDLQMessageMustBeRedeliveredException expectedException
    ) {
        when(context.sender.routeToRedelivery(context.message)).thenReturn(KDLQMessageRouter.RoutingStatus.SCHEDULED_TO_REDELIVERY);
        final var status1 = context.consumer.accept(context.message);
        assertEquals(KDLQMessageConsumer.Status.SCHEDULED_TO_REDELIVERY, status1, "Status must be equal");

        verify(context.sender).routeToRedelivery(context.message);
        verifyNoMoreInteractions(context.sender);
        verify(context.listener).onMessageProcessing(CONSUMER_ID, context.message, KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED, expectedException);
        verifyNoMoreInteractions(context.listener);
    }

    private void execute(
            final Consumer<TestContext> checks,
            final KDLQMessageProcessor<String, byte[]> messageProcessor
    ) {
        execute(checks, messageProcessor, false, mock(Consumer.class));
    }

    private void execute(
            final Consumer<TestContext> checks,
            final KDLQMessageProcessor<String, byte[]> messageProcessor,
            final boolean ignoreExceptionOnClose,
            final Consumer<KDLQMessageConsumer<?, ?>> callback
    ) {
        final KDLQMessageRouter<String, byte[]> sender = RoutersUtil.createRouterMock();
        final var listener = mock(KDLQMessageLifecycleListener.class);
        @SuppressWarnings("unchecked")
        final ConsumerRecord<String, byte[]> message = mock(ConsumerRecord.class);

        final var consumer = createConsumer(listener, message, messageProcessor, sender, callback);
        try (consumer) {
            checks.accept(new TestContext(sender, consumer, message, listener));
        } catch (KDLQLifecycleException ex) {
            if (ignoreExceptionOnClose) {
                return;
            }

            throw ex;
        }

        verify(callback).accept(eq(consumer));
    }

    private ConfigurableKDLQMessageConsumer<String, byte[]> createConsumer(
            final KDLQMessageLifecycleListener listener,
            final ConsumerRecord<String, byte[]> message,
            final KDLQMessageProcessor<String, byte[]> messageProcessor,
            final KDLQMessageRouter<String, byte[]> sender,
            final Consumer<KDLQMessageConsumer<?, ?>> callback
    ) {
        return new ConfigurableKDLQMessageConsumer<>(
                CONSUMER_ID,
                buildConfig(listener),
                m -> {
                    if (m == message) {
                        return messageProcessor.process(m);
                    }

                    throw new AssertionError();
                },
                sender,
                c -> {
                    assertEquals(CONSUMER_ID, c.id(), "Consumer must be same");
                    callback.accept(c);
                }
        );
    }

    private KDLQConfiguration buildConfig(final KDLQMessageLifecycleListener listener) {

        final Map<String, Object> producerProperties = Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
        );

        final var dlqConfig = KDLQConfiguration.DLQ.builder().build("test");

        return ImmutableKDLQConfiguration
                    .builder()
                        .withLifecycleListener(listener)
                    .build(Set.of("srv01"), producerProperties, dlqConfig);
    }

    private record TestContext(
            KDLQMessageRouter<String, byte[]> sender,
            ConfigurableKDLQMessageConsumer<String, byte[]> consumer,
            ConsumerRecord<String, byte[]> message,
            KDLQMessageLifecycleListener listener) {
    }
}
