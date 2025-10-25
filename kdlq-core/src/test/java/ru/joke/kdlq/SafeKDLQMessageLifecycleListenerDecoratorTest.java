package ru.joke.kdlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class SafeKDLQMessageLifecycleListenerDecoratorTest {

    private KDLQMessageLifecycleListener listener;
    private Logger logger;

    private static final String consumerId = "test-consumer";
    private static final ConsumerRecord<String, String> originalMessage = new ConsumerRecord<>("test-topic", 0, 0, "key", "value");
    private static final ProducerRecord<String, String> dlqMessage = new ProducerRecord<>("dlq-topic", "key", "value");
    private static final ProducerRecord<String, String> redeliveryMessage = new ProducerRecord<>("redelivery-topic", "key", "value");
    private static final RuntimeException error = new RuntimeException("Test exception");

    @BeforeEach
    void setUp() {
        this.listener = mock(KDLQMessageLifecycleListener.class);
        this.logger = mock(Logger.class);
    }

    @Test
    void testConstructorWithNullListener() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> new SafeKDLQMessageLifecycleListenerDecorator(null)
        );
    }

    @Test
    void testOnMessageKillSuccessAndExceptionThrows() {
        executeErrorCaseTest((ex, decorator) -> {
            doThrow(ex).when(listener).onMessageKillSuccess(same(consumerId), same(originalMessage), same(dlqMessage));

            decorator.onMessageKillSuccess(consumerId, originalMessage, dlqMessage);

            verify(listener).onMessageKillSuccess(same(consumerId), same(originalMessage), same(dlqMessage));
        });
    }

    @Test
    void testOnMessageKillErrorAndExceptionThrows() {
        executeErrorCaseTest((ex, decorator) -> {
            doThrow(ex).when(listener).onMessageKillError(same(consumerId), same(originalMessage), same(dlqMessage), same(error));

            decorator.onMessageKillError(consumerId, originalMessage, dlqMessage, error);

            verify(listener).onMessageKillError(same(consumerId), same(originalMessage), same(dlqMessage), same(error));
        });
    }

    @Test
    void testOnMessageKillErrorAndNoExceptionThrows() {
        executeSuccessCaseTest(decorator -> {
            decorator.onMessageKillError(consumerId, originalMessage, dlqMessage, error);

            verify(listener).onMessageKillError(same(consumerId), same(originalMessage), same(dlqMessage), same(error));
        });
    }

    @Test
    void testOnMessageKillSuccessAndNoExceptionThrows() {
        executeSuccessCaseTest(decorator -> {
            decorator.onMessageKillSuccess(consumerId, originalMessage, dlqMessage);

            verify(listener).onMessageKillSuccess(same(consumerId), same(originalMessage), same(dlqMessage));
        });
    }

    @Test
    void testOnMessageDiscardErrorAndExceptionThrows() {
        executeErrorCaseTest((ex, decorator) -> {
            doThrow(ex).when(listener).onMessageDiscard(same(consumerId), same(originalMessage));

            decorator.onMessageDiscard(consumerId, originalMessage);

            verify(listener).onMessageDiscard(same(consumerId), same(originalMessage));
        });
    }

    @Test
    void testOnMessageDiscardAndNoExceptionThrows() {
        executeSuccessCaseTest(decorator -> {
            decorator.onMessageDiscard(consumerId, originalMessage);

            verify(listener).onMessageDiscard(same(consumerId), same(originalMessage));
        });
    }

    @Test
    void testOnMessageRedeliverySuccessAndExceptionThrows() {
        executeErrorCaseTest((ex, decorator) -> {
            final var originalMsg = Optional.of(originalMessage);
            doThrow(ex).when(listener).onMessageRedeliverySuccess(same(consumerId), same(originalMsg), same(redeliveryMessage));

            decorator.onMessageRedeliverySuccess(consumerId, originalMsg, redeliveryMessage);

            verify(listener).onMessageRedeliverySuccess(same(consumerId), same(originalMsg), same(redeliveryMessage));
        });
    }

    @Test
    void testOnMessageRedeliverySuccessAndNoExceptionThrows() {
        executeSuccessCaseTest(decorator -> {
            final var originalMsg = Optional.of(originalMessage);
            decorator.onMessageRedeliverySuccess(consumerId, originalMsg, redeliveryMessage);

            verify(listener).onMessageRedeliverySuccess(same(consumerId), same(originalMsg), same(redeliveryMessage));
        });
    }

    @Test
    void testOnMessageRedeliveryErrorAndExceptionThrows() {
        executeErrorCaseTest((ex, decorator) -> {
            final var originalMsg = Optional.of(originalMessage);
            doThrow(ex).when(listener).onMessageRedeliveryError(same(consumerId), same(originalMsg), same(redeliveryMessage), same(error));

            decorator.onMessageRedeliveryError(consumerId, originalMsg, redeliveryMessage, error);

            verify(listener).onMessageRedeliveryError(same(consumerId), same(originalMsg), same(redeliveryMessage), same(error));
        });
    }

    @Test
    void testOnMessageRedeliveryErrorAndNoExceptionThrows() {
        executeSuccessCaseTest(decorator -> {
            final var originalMsg = Optional.of(originalMessage);
            decorator.onMessageRedeliveryError(consumerId, originalMsg, redeliveryMessage, error);

            verify(listener).onMessageRedeliveryError(same(consumerId), same(originalMsg), same(redeliveryMessage), same(error));
        });
    }

    @Test
    void testOnDeferredMessageRedeliverySchedulingSuccessAndExceptionThrows() {
        executeErrorCaseTest((ex, decorator) -> {
            doThrow(ex).when(listener).onDeferredMessageRedeliverySchedulingSuccess(same(consumerId), same(originalMessage), same(redeliveryMessage));

            decorator.onDeferredMessageRedeliverySchedulingSuccess(consumerId, originalMessage, redeliveryMessage);

            verify(listener).onDeferredMessageRedeliverySchedulingSuccess(same(consumerId), same(originalMessage), same(redeliveryMessage));
        });
    }

    @Test
    void testOnDeferredMessageRedeliverySchedulingSuccessAndNoExceptionThrows() {
        executeSuccessCaseTest(decorator -> {
            decorator.onDeferredMessageRedeliverySchedulingSuccess(consumerId, originalMessage, redeliveryMessage);

            verify(listener).onDeferredMessageRedeliverySchedulingSuccess(same(consumerId), same(originalMessage), same(redeliveryMessage));
        });
    }

    @Test
    void testOnDeferredMessageRedeliverySchedulingErrorAndExceptionThrows() {
        executeErrorCaseTest((ex, decorator) -> {
            doThrow(ex).when(listener).onDeferredMessageRedeliverySchedulingError(same(consumerId), same(originalMessage), same(redeliveryMessage), same(error));

            decorator.onDeferredMessageRedeliverySchedulingError(consumerId, originalMessage, redeliveryMessage, error);

            verify(listener).onDeferredMessageRedeliverySchedulingError(same(consumerId), same(originalMessage), same(redeliveryMessage), same(error));
        });
    }

    @Test
    void testOnDeferredMessageRedeliverySchedulingErrorAndNoExceptionThrows() {
        executeSuccessCaseTest(decorator -> {
            decorator.onDeferredMessageRedeliverySchedulingError(consumerId, originalMessage, redeliveryMessage, error);

            verify(listener).onDeferredMessageRedeliverySchedulingError(same(consumerId), same(originalMessage), same(redeliveryMessage), same(error));
        });
    }

    @Test
    void testOnMessageProcessingAndExceptionThrows() {
        executeErrorCaseTest((ex, decorator) -> {
            doThrow(ex).when(listener).onMessageProcessing(same(consumerId), same(originalMessage), same(KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED), same(error));

            decorator.onMessageProcessing(consumerId, originalMessage, KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED, error);

            verify(listener).onMessageProcessing(same(consumerId), same(originalMessage), same(KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED), same(error));
        });
    }

    @Test
    void testOnMessageProcessingAndNoExceptionThrows() {
        executeSuccessCaseTest(decorator -> {
            decorator.onMessageProcessing(consumerId, originalMessage, KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED, error);

            verify(listener).onMessageProcessing(same(consumerId), same(originalMessage), same(KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED), same(error));
        });
    }

    private void executeSuccessCaseTest(final Consumer<SafeKDLQMessageLifecycleListenerDecorator> checks) {
        try (final var loggerFactory = Mockito.mockStatic(LoggerFactory.class)) {
            loggerFactory.when(() -> LoggerFactory.getLogger(this.listener.getClass()))
                         .thenReturn(this.logger);

            final var decorator = new SafeKDLQMessageLifecycleListenerDecorator(this.listener);

            checks.accept(decorator);
            verifyNoInteractions(logger);
        }
    }

    private void executeErrorCaseTest(final BiConsumer<RuntimeException, SafeKDLQMessageLifecycleListenerDecorator> checks) {
        try (final var loggerFactory = Mockito.mockStatic(LoggerFactory.class)) {
            loggerFactory.when(() -> LoggerFactory.getLogger(this.listener.getClass()))
                         .thenReturn(this.logger);

            final RuntimeException ex = new RuntimeException();
            final var decorator = new SafeKDLQMessageLifecycleListenerDecorator(this.listener);

            checks.accept(ex, decorator);
            verify(logger).warn(eq(""), eq(ex));
        }
    }
}
