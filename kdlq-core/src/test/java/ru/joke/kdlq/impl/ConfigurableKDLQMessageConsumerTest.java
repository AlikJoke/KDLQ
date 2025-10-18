package ru.joke.kdlq.impl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.*;
import ru.joke.kdlq.internal.consumers.ConfigurableKDLQMessageConsumer;
import ru.joke.kdlq.internal.routers.KDLQMessageRouter;
import ru.joke.kdlq.impl.internal.TestUtil;

import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ConfigurableKDLQMessageConsumerTest {

    private static final String CONSUMER_ID = "test";

    @Test
    public void testWhenProcessingSuccess() {
        execute(
                this::makeSuccessChecks,
                m -> KDLQMessageProcessor.ProcessingStatus.OK
        );
    }

    @Test
    public void testWhenProcessingFailedThenSendToDLQShouldBeInvoked() {
        execute(
                context -> makeUnrecoverableErrorChecks(context, null),
                m -> KDLQMessageProcessor.ProcessingStatus.ERROR
        );
        final var handlerError = new RuntimeException();
        execute(
                context -> makeUnrecoverableErrorChecks(context, handlerError),
                m -> {
                    throw handlerError;
                }
        );
    }

    @Test
    public void testWhenProcessingFinishedWithRedeliveryThenRedeliveryShouldBeInvoked() {
        execute(
                context -> makeRedeliveryErrorChecks(context, null),
                m -> KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED
        );
        final var handlerError = new KDLQMessageMustBeRedeliveredException("some cause");
        execute(
                context -> makeRedeliveryErrorChecks(context, handlerError),
                m -> {
                    throw handlerError;
                }
        );
    }

    @Test
    public void testWhenConsumerClosed() {
        final Runnable closeChecks =
                () -> execute(
                        context -> {
                            context.consumer.close();
                            assertThrows(KDLQLifecycleException.class, context.consumer::close);
                            assertThrows(KDLQLifecycleException.class, () -> context.consumer.accept(context.message));
                        },
                        m -> KDLQMessageProcessor.ProcessingStatus.OK,
                        true
                );
    }

    private void makeUnrecoverableErrorChecks(final TestContext context, final RuntimeException expectedHandlerError) {
        when(context.sender.routeToDLQ(context.message)).thenReturn(true);
        final var status1 = context.consumer.accept(context.message);

        when(context.sender.routeToDLQ(context.message)).thenReturn(false);
        final var status2 = context.consumer.accept(context.message);

        assertEquals(KDLQMessageConsumer.Status.ROUTED_TO_DLQ, status1, "Status must be equal");
        assertEquals(KDLQMessageConsumer.Status.SKIPPED_DLQ_MAX_ATTEMPTS_REACHED, status2, "Status must be equal");

        verify(context.sender, times(2)).routeToDLQ(context.message);
        verifyNoMoreInteractions(context.sender);
        verify(context.listener, times(2)).onMessageProcessing(CONSUMER_ID, context.message, KDLQMessageProcessor.ProcessingStatus.ERROR, expectedHandlerError);
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

    private void makeRedeliveryErrorChecks(final TestContext context, final KDLQMessageMustBeRedeliveredException expectedException) {
        when(context.sender.routeToRedelivery(context.message)).thenReturn(true);
        final var status1 = context.consumer.accept(context.message);
        assertEquals(KDLQMessageConsumer.Status.REDELIVERED, status1, "Status must be equal");

        when(context.sender.routeToRedelivery(context.message)).thenReturn(false);
        final var status2 = context.consumer.accept(context.message);
        assertEquals(KDLQMessageConsumer.Status.ROUTED_TO_DLQ, status2, "Status must be equal");

        verify(context.sender, times(2)).routeToRedelivery(context.message);
        verifyNoMoreInteractions(context.sender);
        verify(context.listener, times(2)).onMessageProcessing(CONSUMER_ID, context.message, KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED, expectedException);
        verifyNoMoreInteractions(context.listener);
    }

    private void execute(
            final Consumer<TestContext> checks,
            final KDLQMessageProcessor<String, byte[]> messageProcessor) {
        execute(checks, messageProcessor, false);
    }

    private void execute(
            final Consumer<TestContext> checks,
            final KDLQMessageProcessor<String, byte[]> messageProcessor,
            final boolean ignoreExceptionOnClose) {

        final KDLQMessageRouter<String, byte[]> sender = TestUtil.createSender();
        final var listener = mock(KDLQMessageLifecycleListener.class);
        @SuppressWarnings("unchecked")
        final ConsumerRecord<String, byte[]> message = mock(ConsumerRecord.class);

        try (final ConfigurableKDLQMessageConsumer<String, byte[]> consumer = createConsumer(listener, message, messageProcessor, sender)) {
            checks.accept(new TestContext(sender, consumer, message, listener));
        } catch (KDLQLifecycleException ex) {
            if (ignoreExceptionOnClose) {
                return;
            }

            throw ex;
        }
    }

    private ConfigurableKDLQMessageConsumer<String, byte[]> createConsumer(
            final KDLQMessageLifecycleListener listener,
            final ConsumerRecord<String, byte[]> message,
            final KDLQMessageProcessor<String, byte[]> messageProcessor,
            final KDLQMessageRouter<String, byte[]> sender) {
        return new ConfigurableKDLQMessageConsumer<>(
                CONSUMER_ID,
                buildConfig(listener),
                m -> {
                    if (m == message) {
                        return messageProcessor.process(m);
                    }

                    throw new AssertionError();
                },
                sender
        );
    }

    private KDLQConfiguration buildConfig(final KDLQMessageLifecycleListener listener) {
        return ImmutableKDLQConfiguration
                    .builder()
                        .withLifecycleListener(listener)
                    .build(Set.of("srv01"), "test");
    }

    private record TestContext(
            KDLQMessageRouter<String, byte[]> sender,
            ConfigurableKDLQMessageConsumer<String, byte[]> consumer,
            ConsumerRecord<String, byte[]> message,
            KDLQMessageLifecycleListener listener) {
    }
}
