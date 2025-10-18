package ru.joke.kdlq.mongo;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.internal.thread.DaemonThreadFactory;
import org.bson.Document;
import ru.joke.kdlq.ImmutableKDLQGlobalConfiguration;
import ru.joke.kdlq.KDLQConfigurationException;
import ru.joke.kdlq.KDLQGlobalConfiguration;
import ru.joke.kdlq.internal.util.Args;
import ru.joke.kdlq.mongo.internal.KDLQMongoGlobalDistributedLockService;
import ru.joke.kdlq.mongo.internal.KDLQRedeliveryMongoStorage;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public final class KDLQMongoGlobalConfigurationBuilder {

    private static final String DEFAULT_LOCKS_COLLECTION_NAME = "distributed_locks";
    private static final String DEFAULT_REDELIVERY_COLLECTION_NAME = "redelivery_queue";

    private ScheduledExecutorService lockingPool;
    private ScheduledExecutorService redeliveryPool;
    private long redeliveryTaskDelay = 1_000;
    private MongoClient mongoClient;
    private String databaseName;
    private String distributedLocksCollectionName;
    private String redeliveryQueueCollectionName;

    public KDLQMongoGlobalConfigurationBuilder withRedeliveryPool(ScheduledExecutorService redeliveryPool) {
        this.redeliveryPool = redeliveryPool;
        return this;
    }

    public KDLQMongoGlobalConfigurationBuilder withLockingPool(ScheduledExecutorService lockingPool) {
        this.lockingPool = lockingPool;
        return this;
    }

    public KDLQMongoGlobalConfigurationBuilder withRedeliveryTaskDelay(long redeliveryTaskDelay) {
        this.redeliveryTaskDelay = redeliveryTaskDelay;
        return this;
    }

    public KDLQMongoGlobalConfigurationBuilder withMongoClient(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        return this;
    }

    public KDLQMongoGlobalConfigurationBuilder withDistributedLocksCollection(String distributedLocksCollectionName) {
        this.distributedLocksCollectionName = distributedLocksCollectionName;
        return this;
    }

    public KDLQMongoGlobalConfigurationBuilder withRedeliveryQueueCollection(String redeliveryQueueCollectionName) {
        this.redeliveryQueueCollectionName = redeliveryQueueCollectionName;
        return this;
    }

    public KDLQGlobalConfiguration build() {
        final var mongoClient = Args.requireNotNull(this.mongoClient, () -> new KDLQConfigurationException("MongoClient must be provided"));
        final var databaseName = Args.requireNotEmpty(this.databaseName, () -> new KDLQConfigurationException("Database must be not empty"));
        final var db = mongoClient.getDatabase(databaseName);

        final String locksCollectionName =
                this.distributedLocksCollectionName == null
                        ? DEFAULT_LOCKS_COLLECTION_NAME
                        : this.distributedLocksCollectionName;
        final String redeliveryCollectionName =
                this.redeliveryQueueCollectionName == null
                        ? DEFAULT_REDELIVERY_COLLECTION_NAME
                        : this.redeliveryQueueCollectionName;
        final var locksCollection = findOrCreateCollection(db, locksCollectionName);

        final var lockingPool =
                this.lockingPool == null
                        ? Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("lock-renewal"))
                        : this.lockingPool;

        final var distributedLockService = new KDLQMongoGlobalDistributedLockService(locksCollection, lockingPool);

        final var redeliveryCollection = findOrCreateCollection(db, redeliveryCollectionName);
        final var redeliveryStorage = new KDLQRedeliveryMongoStorage(redeliveryCollection);

        return ImmutableKDLQGlobalConfiguration.builder()
                                                    .withDistributedLockService(distributedLockService)
                                                    .withRedeliveryStorage(redeliveryStorage)
                                                    .withRedeliveryTaskDelay(this.redeliveryTaskDelay)
                                                    .withRedeliveryPool(redeliveryPool)
                                               .build();
    }

    public static KDLQMongoGlobalConfigurationBuilder create() {
        return new KDLQMongoGlobalConfigurationBuilder();
    }

    private KDLQMongoGlobalConfigurationBuilder() {}

    private MongoCollection<Document> findOrCreateCollection(
            final MongoDatabase db,
            final String collectionName
    ) {
        try {
            db.createCollection(collectionName);
        } catch (MongoWriteException ignored) {
        }

        return db.getCollection(collectionName);
    }
}
