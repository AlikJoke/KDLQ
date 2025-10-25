package ru.joke.kdlq.internal.configs;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.ImmutableKDLQConfiguration;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQProducerRecord;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InMemoryKDLQRedeliveryStorageTest {

    private final KDLQRedeliveryStorage storage = new InMemoryKDLQRedeliveryStorage();
    private KDLQConfiguration config;

    @BeforeEach
    void setUp() {
        this.config = mock(ImmutableKDLQConfiguration.class);
        when(this.config.id()).thenReturn("cfg");
    }

    @Test
    void testStoreRecordSuccess() {
        final var redeliveryTimestamp = 100L;
        final var record = createMockKDLQRecord(redeliveryTimestamp);
        this.storage.store(record);

        final var found = this.storage.findAllReadyToRedelivery(
                s -> this.config, redeliveryTimestamp, 10
        );

        assertEquals(1, found.size(), "Record must be found");
        assertEquals(redeliveryTimestamp, found.getFirst().nextRedeliveryTimestamp(), "Redelivery timestamp must be equal");
        assertNotNull(found.getFirst().id(), "Id must be not null");
    }

    @Test
    void testStoreTwoRecords() {
        final var redeliveryTimestamp = 100L;
        final var record1 = createMockKDLQRecord(redeliveryTimestamp);
        final var record2 = createMockKDLQRecord(redeliveryTimestamp);

        storage.store(record1);
        storage.store(record2);

        final var found = storage.findAllReadyToRedelivery(
                s -> this.config, redeliveryTimestamp, 10
        );

        assertEquals(2, found.size());
        assertNotEquals(found.getFirst().id(), found.get(1).id(), "Ids of the records must be different");
    }

    @Test
    void testDeleteByIdSuccess() {
        final var record1 = createMockKDLQRecord(100L);
        final var record2 = createMockKDLQRecord(110L);
        final var record3 = createMockKDLQRecord(120L);

        storage.store(record1);
        storage.store(record2);
        storage.store(record3);

        final var allRecords = storage.findAllReadyToRedelivery(
                s -> this.config, Long.MAX_VALUE, 10
        );
        assertEquals(3, allRecords.size(), "Count of records must be equal");

        final var idToDelete = allRecords.getFirst().id();
        storage.deleteById(idToDelete);

        final var remainingRecords = storage.findAllReadyToRedelivery(
                s -> this.config, Long.MAX_VALUE, 10
        );
        assertEquals(2, remainingRecords.size(), "Count of records must be equal");

        final var deletedRecordMustBeAbsent = remainingRecords.stream().noneMatch(r -> r.id().equals(idToDelete));
        assertTrue(deletedRecordMustBeAbsent, "Deleted record must not present in remaining records");
    }

    @Test
    void testWhenDeleteByIdNonExistentIdThenNoErrors() {
        final var record1 = createMockKDLQRecord(100L);
        storage.store(record1);

        final int initialSize = storage.findAllReadyToRedelivery(
                s -> this.config, Long.MAX_VALUE, 10
        ).size();
        assertEquals(1, initialSize);

        storage.deleteById("1");

        final int finalSize = storage.findAllReadyToRedelivery(
                s -> this.config, Long.MAX_VALUE, 10
        ).size();
        assertEquals(initialSize, finalSize, "Count of records must be equal");
    }

    @Test
    void testWhenDeleteByIdsEmptySetThenNoChanges() {
        final var record1 = createMockKDLQRecord(100L);
        storage.store(record1);

        final int initialSize = storage.findAllReadyToRedelivery(
                s -> this.config, Long.MAX_VALUE, 10
        ).size();
        assertEquals(1, initialSize, "Count of records must be equal");

        storage.deleteByIds(Collections.emptySet());

        final int finalSize = storage.findAllReadyToRedelivery(
                s -> this.config, Long.MAX_VALUE, 10
        ).size();
        assertEquals(initialSize, finalSize, "Count of record must be equal");
    }

    @Test
    void testFindAllReadyToRedeliveryWithFiltersByTimestamp() {
        final var ts1 = 50L;
        final var ts2 = 100L;
        final var ts3 = 150L;
        storage.store(createMockKDLQRecord(ts1));
        storage.store(createMockKDLQRecord(ts2));
        storage.store(createMockKDLQRecord(ts3));

        final var readyRecords = storage.findAllReadyToRedelivery(
                s -> this.config, 100L, 10
        );

        assertEquals(2, readyRecords.size(), "Count of records must be equal");
        assertEquals(ts1, readyRecords.getFirst().nextRedeliveryTimestamp(), "Redelivery timestamp must be equal");
        assertEquals(ts2, readyRecords.get(1).nextRedeliveryTimestamp(), "Redelivery timestamp must be equal");
    }

    @Test
    void testFindAllReadyToRedeliveryWithSortsByTimestamp() {
        final var ts1 = 50L;
        final var ts2 = 100L;
        final var ts3 = 150L;

        storage.store(createMockKDLQRecord(ts3));
        storage.store(createMockKDLQRecord(ts1));
        storage.store(createMockKDLQRecord(ts2));

        final var readyRecords = storage.findAllReadyToRedelivery(
                s -> this.config, ts3 + 1, 10
        );

        assertEquals(3, readyRecords.size(), "Count of records must be equal");
        assertEquals(ts1, readyRecords.getFirst().nextRedeliveryTimestamp(), "Redelivery timestamp must be equal");
        assertEquals(ts2, readyRecords.get(1).nextRedeliveryTimestamp(), "Redelivery timestamp must be equal");
        assertEquals(ts3, readyRecords.get(2).nextRedeliveryTimestamp(), "Redelivery timestamp must be equal");
    }

    @Test
    void testFindAllReadyToRedeliveryAppliesLimit() {
        final var ts1 = 50L;
        storage.store(createMockKDLQRecord(ts1));

        final var ts2 = 60L;
        storage.store(createMockKDLQRecord(ts2));

        final var ts3 = 70L;
        storage.store(createMockKDLQRecord(ts3));

        final var ts4 = 80L;
        storage.store(createMockKDLQRecord(ts4));

        final var ts5 = 90L;
        storage.store(createMockKDLQRecord(ts5));

        final var readyRecords = storage.findAllReadyToRedelivery(
                s -> this.config, ts5 + 1, 3
        );

        assertEquals(3, readyRecords.size(), "Count of records must be equal");
        assertEquals(ts1, readyRecords.getFirst().nextRedeliveryTimestamp(), "Redelivery timestamp must be equal");
        assertEquals(ts2, readyRecords.get(1).nextRedeliveryTimestamp(), "Redelivery timestamp must be equal");
        assertEquals(ts3, readyRecords.get(2).nextRedeliveryTimestamp(), "Redelivery timestamp must be equal");
    }

    @Test
    void testFindAllReadyToRedeliveryWhenNoRecordsReadyThenReturnsEmptyList() {
        storage.store(createMockKDLQRecord(150L));
        storage.store(createMockKDLQRecord(200L));

        final var readyRecords = storage.findAllReadyToRedelivery(
                s -> this.config, 100L, 10
        );

        assertTrue(readyRecords.isEmpty(), "Result list must be empty");
    }

    @Test
    void testFindAllReadyToRedeliveryWhenEmptyStorageThenReturnsEmptyList() {
        final var readyRecords = storage.findAllReadyToRedelivery(
                s -> this.config, 100L, 10
        );
        assertTrue(readyRecords.isEmpty(), "Result list be empty");
    }

    @Test
    void testStoredRecordData() {
        final var original = createMockKDLQRecord(200L);
        storage.store(original);
        final var records = storage.findAllReadyToRedelivery(
                s -> this.config, Long.MAX_VALUE, 10
        );
        
        assertEquals(1, records.size(), "Count of records must be equal");
        
        assertSame(original.record(), records.getFirst().record(), "Record must be same");
        assertSame(original.configuration(), records.getFirst().configuration(), "Configuration must be same");
        assertEquals(original.nextRedeliveryTimestamp(), records.getFirst().nextRedeliveryTimestamp(), "Redelivery timestamp must be equal");
    }

    private KDLQProducerRecord<byte[], byte[]> createMockKDLQRecord(long redeliveryTimestamp) {
        final var record = new ProducerRecord<>("topic", "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8));
        return new KDLQProducerRecord<byte[], byte[]>() {
            @Nonnull
            @Override
            public ProducerRecord<byte[], byte[]> record() {
                return record;
            }

            @Override
            public KDLQConfiguration configuration() {
                return config;
            }

            @Override
            public long nextRedeliveryTimestamp() {
                return redeliveryTimestamp;
            }
        };
    }
}
