package ru.joke.kdlq.internal.configs;

import org.apache.kafka.clients.producer.ProducerRecord;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQProducerRecord;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@ThreadSafe
final class InMemoryKDLQRedeliveryStorage implements KDLQRedeliveryStorage {

    private final Set<KDLQProducerRecord.Identifiable<byte[], byte[]>> storage = ConcurrentHashMap.newKeySet();

    @Override
    public void store(@Nonnull KDLQProducerRecord<byte[], byte[]> obj) {
        this.storage.add(new IdentifiableRecord(obj));
    }

    @Override
    public void deleteByIds(@Nonnull Set<String> objectIds) {
        this.storage.removeIf(r -> objectIds.contains(r.id()));
    }

    @Nonnull
    @Override
    public List<KDLQProducerRecord.Identifiable<byte[], byte[]>> findAllReadyToRedelivery(
            @Nonnull final Function<String, KDLQConfiguration> configurationFactory,
            @Nonnegative final long redeliveryTimestamp,
            @Nonnull final int limit
    ) {
        return this.storage.stream()
                            .filter(r -> r.nextRedeliveryTimestamp() <= redeliveryTimestamp)
                            .sorted(Comparator.comparingLong(KDLQProducerRecord::nextRedeliveryTimestamp))
                            .limit(limit)
                            .collect(Collectors.toList());
    }

    private static class IdentifiableRecord implements KDLQProducerRecord.Identifiable<byte[], byte[]> {

        private final KDLQProducerRecord<byte[], byte[]> original;
        private final String id;

        private IdentifiableRecord(final KDLQProducerRecord<byte[], byte[]> original) {
            this.original = original;
            this.id = UUID.randomUUID().toString();
        }

        @Nonnull
        @Override
        public ProducerRecord<byte[], byte[]> record() {
            return this.original.record();
        }

        @Override
        public KDLQConfiguration configuration() {
            return this.original.configuration();
        }

        @Override
        public long nextRedeliveryTimestamp() {
            return this.original.nextRedeliveryTimestamp();
        }

        @Nonnull
        @Override
        public String id() {
            return this.id;
        }
    }
}
