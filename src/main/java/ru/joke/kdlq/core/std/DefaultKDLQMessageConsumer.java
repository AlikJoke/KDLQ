package ru.joke.kdlq.core.std;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.joke.kdlq.core.KDLQConfiguration;
import ru.joke.kdlq.core.KDLQMessageConsumer;
import ru.joke.kdlq.core.internal.KDLQMessageSender;
import ru.joke.kdlq.core.internal.KDLQMessageSenderHolder;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.function.Predicate;

public class DefaultKDLQMessageConsumer implements KDLQMessageConsumer, Closeable {

    private final KDLQMessageSender messageSender;
    private final KDLQConfiguration dlqConfiguration;

    public DefaultKDLQMessageConsumer(@Nonnull KDLQConfiguration dlqConfiguration) {
        this.dlqConfiguration = dlqConfiguration;
        this.messageSender = KDLQMessageSenderHolder.get();
    }

    @Override
    @Nonnull
    public <K, V> KDLQMessageConsumer.Status accept(@Nonnull ConsumerRecord<K, V> message, @Nonnull Predicate<ConsumerRecord<K, V>> action) {
        if (action.test(message)) {
            return KDLQMessageConsumer.Status.OK;
        }

        final boolean sent = this.messageSender.sendIfNeed(message, this.dlqConfiguration);
        return sent ? KDLQMessageConsumer.Status.ERROR_DLQ_OK : KDLQMessageConsumer.Status.ERROR_RETRY;
    }

    @Override
    public void close() {
        this.messageSender.close(this.dlqConfiguration);
    }
}
