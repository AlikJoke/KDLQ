package ru.joke.kdlq.core.std;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.joke.kdlq.core.KDLQConfiguration;
import ru.joke.kdlq.core.KDLQMessageConsumer;
import ru.joke.kdlq.core.KDLQMessageMustBeRedeliveredException;
import ru.joke.kdlq.core.KDLQMessageProcessor;
import ru.joke.kdlq.core.internal.KDLQMessageSender;

import javax.annotation.Nonnull;
import java.io.Closeable;

public final class DefaultKDLQMessageConsumer<K, V> implements KDLQMessageConsumer<K, V>, Closeable {

    private final String id;
    private final KDLQMessageSender<K, V> messageSender;
    private final KDLQConfiguration dlqConfiguration;

    public DefaultKDLQMessageConsumer(@Nonnull String id, @Nonnull KDLQConfiguration dlqConfiguration) {
        this.id = id;
        this.dlqConfiguration = dlqConfiguration;
        this.messageSender = new KDLQMessageSender<>(id, dlqConfiguration);
    }

    @Override
    @Nonnull
    public Status accept(@Nonnull ConsumerRecord<K, V> message, @Nonnull KDLQMessageProcessor<K, V> messageProcessor) {
        return switch (process(message, messageProcessor)) {
            case OK -> Status.OK;
            case MUST_BE_REDELIVERED -> this.messageSender.redeliver(message)
                                                ? Status.WILL_BE_REDELIVERED
                                                : Status.ERROR_DLQ_OK;
            case ERROR -> this.messageSender.sendToDLQ(message)
                                                ? Status.ERROR_DLQ_OK
                                                : Status.ERROR_DLQ_MAX_ATTEMPTS_REACHED;
        };
    }

    @Override
    public void close() {
        this.messageSender.close();
    }

    private KDLQMessageProcessor.ProcessingStatus process(@Nonnull ConsumerRecord<K, V> message, @Nonnull KDLQMessageProcessor<K, V> messageProcessor) {
        KDLQMessageProcessor.ProcessingStatus processingStatus;
        RuntimeException processingError = null;
        try {
            processingStatus = messageProcessor.process(message);
        } catch (KDLQMessageMustBeRedeliveredException ex) {
            processingStatus = KDLQMessageProcessor.ProcessingStatus.MUST_BE_REDELIVERED;
            processingError = ex;
        } catch (RuntimeException ex) {
            processingStatus = KDLQMessageProcessor.ProcessingStatus.ERROR;
            processingError = ex;
        }

        callListeners(message, processingStatus, processingError);

        return processingStatus;
    }

    private void callListeners(
            final ConsumerRecord<K, V> message,
            final KDLQMessageProcessor.ProcessingStatus processingStatus,
            final RuntimeException processingError) {
        this.dlqConfiguration.lifecycleListeners().forEach(l -> {
            if (processingError != null) {
                l.onErrorMessageProcessing(this.id, message, processingStatus, processingError);
            } else {
                l.onSuccessMessageProcessing(this.id, message, processingStatus);
            }
        });
    }
}
