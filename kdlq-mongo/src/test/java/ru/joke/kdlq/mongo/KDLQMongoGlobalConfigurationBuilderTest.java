package ru.joke.kdlq.mongo;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.KDLQConfigurationException;
import ru.joke.kdlq.KDLQGlobalConfiguration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class KDLQMongoGlobalConfigurationBuilderTest {

    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection<Document> mongoCollection;
    private ScheduledExecutorService lockingPool;
    private ScheduledExecutorService redeliveryDispatcherPool;
    private ExecutorService redeliveryPool;

    private KDLQMongoGlobalConfigurationBuilder builder;

    private final String databaseName = "test_db";

    @BeforeEach
    void setUp() {
        this.mongoClient = mock(MongoClient.class);
        this.mongoDatabase = mock(MongoDatabase.class);
        this.mongoCollection = mock(MongoCollection.class);
        this.lockingPool = mock(ScheduledExecutorService.class);
        this.redeliveryDispatcherPool = mock(ScheduledExecutorService.class);
        this.redeliveryPool = mock(ExecutorService.class);
        this.builder = KDLQMongoGlobalConfigurationBuilder.create();
        when(mongoClient.getDatabase(databaseName)).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(mongoCollection);
    }

    @Test
    void testCreateReturnsNewBuilderInstance() {
        final var builder = KDLQMongoGlobalConfigurationBuilder.create();
        assertNotNull(builder, "Builder must be not null");
        assertNotSame(builder, this.builder, "Builder instances must be different");
    }

    @Test
    void testBuildWithCustomPools() {
        final var expectedDelay = 10L;
        final var config =
                builder
                    .withRedeliveryDispatcherPool(redeliveryDispatcherPool)
                    .withRedeliveryPool(redeliveryPool)
                    .withLockingPool(lockingPool)
                    .withRedeliveryDispatcherTaskDelay(expectedDelay)
                .build(mongoClient, databaseName);

        assertNotNull(config, "Created config must be not null");
        assertEquals(expectedDelay, config.redeliveryDispatcherTaskDelay(), "Redelivery dispatcher task delay must be equal");
        assertSame(redeliveryDispatcherPool, config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be same");
        assertSame(redeliveryPool, config.redeliveryPool(), "Redelivery pool must be same");
        assertNotNull(config.redeliveryStorage(), "Redelivery storage must be not null");
        assertNotNull(config.distributedLockService(), "Lock service must be not null");
    }

    @Test
    void testBuildWithCustomCollectionNames() {
        final var locksCollectionName = "custom_locks";
        final var queueCollectionName = "custom_queue";

        when(mongoDatabase.getCollection(locksCollectionName, Document.class)).thenReturn(mongoCollection);
        when(mongoDatabase.getCollection(queueCollectionName, Document.class)).thenReturn(mongoCollection);

        final var config =
                builder
                        .withDistributedLocksCollection(locksCollectionName)
                        .withRedeliveryQueueCollection(queueCollectionName)
                .build(mongoClient, databaseName);

        assertNotNull(config, "Created config must be not null");
        verify(mongoDatabase).getCollection(locksCollectionName);
        verify(mongoDatabase).getCollection(queueCollectionName);
    }

    @Test
    void testBuildWithDefaultPoolsAndCollectionNames() {
        final var config = builder.build(mongoClient, databaseName);

        assertNotNull(config, "Created config must be not null");
        assertNotNull(config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be not null");
        assertNotNull(config.redeliveryPool(), "Redelivery pool must be not null");

        verify(mongoDatabase).getCollection("distributed_locks");
        verify(mongoDatabase).getCollection("redelivery_queue");
    }

    @Test
    void testBuildWithNullMongoClient() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> builder.build(null, databaseName)
        );
    }

    @Test
    void testBuildWithInvalidDatabaseName() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> builder.build(mongoClient, null)
        );
        assertThrows(
                KDLQConfigurationException.class,
                () -> builder.build(mongoClient, "")
        );
    }

    @Test
    void testFindOrCreateCollectionWhenCollectionExists() {
        final var collName = "coll";
        final var exception = mock(MongoWriteException.class);
        doThrow(exception).when(mongoDatabase).createCollection(collName);

        final var config =
                builder
                        .withRedeliveryQueueCollection(collName)
                .build(mongoClient, databaseName);
        verify(mongoDatabase).createCollection(collName);
        verify(mongoDatabase).getCollection(collName);
    }
}
