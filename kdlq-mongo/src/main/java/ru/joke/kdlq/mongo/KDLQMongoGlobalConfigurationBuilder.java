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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * KDLQ Global Configuration Builder for MongoDB-backed redelivery message storage.
 *
 * @author Alik
 * @see KDLQGlobalConfiguration
 * @see ru.joke.kdlq.spi.KDLQRedeliveryStorage
 * @see ru.joke.kdlq.spi.KDLQGlobalDistributedLockService
 */
@NotThreadSafe
public final class KDLQMongoGlobalConfigurationBuilder {

    private static final String DEFAULT_LOCKS_COLLECTION_NAME = "distributed_locks";
    private static final String DEFAULT_REDELIVERY_COLLECTION_NAME = "redelivery_queue";

    private ScheduledExecutorService lockingPool;
    private ScheduledExecutorService redeliveryDispatcherPool;
    private ExecutorService redeliveryPool;
    private long redeliveryDispatcherTaskDelay = 1_000;
    private String distributedLocksCollectionName;
    private String redeliveryQueueCollectionName;

    /**
     * Sets the thread pool for running dispatcher of the message redelivery tasks.
     *
     * @param redeliveryDispatcherPool redelivery dispatcher pool; if pool is not provided, a single-threaded
     *                                 pool managed by KDLQ will be created.
     * @return the current builder object for further construction; cannot be {@code null}.
     * @see KDLQGlobalConfiguration#redeliveryDispatcherPool()
     */
    @Nonnull
    public KDLQMongoGlobalConfigurationBuilder withRedeliveryDispatcherPool(ScheduledExecutorService redeliveryDispatcherPool) {
        this.redeliveryDispatcherPool = redeliveryDispatcherPool;
        return this;
    }

    /**
     * Sets the thread pool for running the message redelivery tasks.
     *
     * @param redeliveryPool redelivery pool; if pool is not provided, a {@link Executors#newWorkStealingPool()}
     *                       pool managed by KDLQ will be created.
     * @return the current builder object for further construction; cannot be {@code null}.
     * @see KDLQGlobalConfiguration#redeliveryPool()
     */
    @Nonnull
    public KDLQMongoGlobalConfigurationBuilder withRedeliveryPool(ExecutorService redeliveryPool) {
        this.redeliveryPool = redeliveryPool;
        return this;
    }

    /**
     * Sets the thread pool for the global lock synchronization task, which enables
     * the message redelivery task to function within a distributed system.
     *
     * @param lockingPool locking pool; if pool is not provided, a single-threaded
     *                    pool managed by KDLQ will be created.
     * @return the current builder object for further construction; cannot be {@code null}.
     * @see ru.joke.kdlq.spi.KDLQGlobalDistributedLockService
     */
    @Nonnull
    public KDLQMongoGlobalConfigurationBuilder withLockingPool(ScheduledExecutorService lockingPool) {
        this.lockingPool = lockingPool;
        return this;
    }

    /**
     * Sets the delay in milliseconds for the message redelivery task.
     *
     * @param redeliveryDispatcherTaskDelay task delay in milliseconds; if delay is not provided,
     *                            a default value will be applied (1s).
     * @return the current builder object for further construction; cannot be {@code null}.
     * @see KDLQGlobalConfiguration#redeliveryDispatcherTaskDelay()
     */
    @Nonnull
    public KDLQMongoGlobalConfigurationBuilder withRedeliveryDispatcherTaskDelay(@Nonnegative long redeliveryDispatcherTaskDelay) {
        this.redeliveryDispatcherTaskDelay = redeliveryDispatcherTaskDelay;
        return this;
    }

    /**
     * Sets the name of the collection that will store global distributed lock
     * information for determining which server will execute the message redelivery task.
     *
     * @param distributedLocksCollectionName collection to store global distributed lock;
     *                                       if collection name is not provided, a default
     *                                       value will be applied: 'distributed_locks'.
     * @return the current builder object for further construction; cannot be {@code null}.
     * @see ru.joke.kdlq.spi.KDLQGlobalDistributedLockService
     */
    @Nonnull
    public KDLQMongoGlobalConfigurationBuilder withDistributedLocksCollection(String distributedLocksCollectionName) {
        this.distributedLocksCollectionName = distributedLocksCollectionName;
        return this;
    }

    /**
     * Sets the name of the collection where information about messages to be
     * redelivered will be stored.
     *
     * @param redeliveryQueueCollectionName collection to store messages to be redelivered;
     *                                      if collection name is not provided, a default
     *                                      value will be applied: 'redelivery_queue'.
     * @return the current builder object for further construction; cannot be {@code null}.
     * @see ru.joke.kdlq.spi.KDLQRedeliveryStorage
     */
    @Nonnull
    public KDLQMongoGlobalConfigurationBuilder withRedeliveryQueueCollection(String redeliveryQueueCollectionName) {
        this.redeliveryQueueCollectionName = redeliveryQueueCollectionName;
        return this;
    }

    /**
     * Creates a global configuration object with the specified parameters.<br>
     * Mandatory parameters are passed as arguments to this method; others are optional,
     * and if not explicitly set in the builder, default values will be used.
     *
     * @param mongoClient  client to MongoDB; cannot be {@code null}.
     * @param databaseName name of the database where collections containing data
     *                     required for KDLQ operation will be located; cannot be {@code null}.
     * @return created configuration object; cannot be {@code null}.
     */
    @Nonnull
    public KDLQGlobalConfiguration build(
            @Nonnull MongoClient mongoClient,
            @Nonnull String databaseName
    ) {
        Args.requireNotNull(mongoClient, () -> new KDLQConfigurationException("MongoClient must be provided"));
        Args.requireNotEmpty(databaseName, () -> new KDLQConfigurationException("Database must be not empty"));
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
                                                    .withRedeliveryDispatcherTaskDelay(this.redeliveryDispatcherTaskDelay)
                                                    .withRedeliveryDispatcherPool(this.redeliveryDispatcherPool)
                                                    .withRedeliveryPool(this.redeliveryPool)
                                               .build(distributedLockService, redeliveryStorage);
    }

    /**
     * Returns a builder instance for more convenient construction of the configuration object.
     *
     * @return builder instance; cannot be {@code null}.
     */
    @Nonnull
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
