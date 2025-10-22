package ru.joke.kdlq.mongo.internal;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.KDLQConfigurationException;
import ru.joke.kdlq.internal.util.Args;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * MongoDB-based implementation of the {@link KDLQGlobalDistributedLockService}.
 *
 * @author Alik
 * @see KDLQGlobalDistributedLockService
 */
@ThreadSafe
public final class KDLQMongoGlobalDistributedLockService implements KDLQGlobalDistributedLockService {

    private static final Logger logger = LoggerFactory.getLogger(KDLQMongoGlobalDistributedLockService.class);

    private static final String LOCK_ID = KDLQMongoGlobalDistributedLockService.class.getSimpleName();
    private static final int LOCK_EXPIRATION_PERIOD = 60;

    private final MongoCollection<Document> locksCollection;
    private final ScheduledExecutorService lockingPool;
    private final String ownerId;
    private volatile Future<?> renewalTask;

    private static final String LOCK_ID_FIELD = "_id";
    private static final String EXPIRES_AT_FIELD = "expiresAt";
    private static final String CREATED_AT_FIELD = "createdAt";
    private static final String OWNER_ID_FIELD = "owner";

    /**
     * Constructs redelivery storage.
     *
     * @param locksCollection collection with locks; cannot be {@code null}.
     * @param lockingPool     thread-pool for the global lock synchronization
     *                        task; cannot be {@code null}.
     */
    public KDLQMongoGlobalDistributedLockService(
            @Nonnull final MongoCollection<Document> locksCollection,
            @Nonnull final ScheduledExecutorService lockingPool
    ) {
        this.locksCollection = Args.requireNotNull(locksCollection, () -> new KDLQConfigurationException("Provided collection must be not null"));
        this.lockingPool = Args.requireNotNull(lockingPool, () -> new KDLQConfigurationException("Provided locking pool be not null"));
        this.ownerId = UUID.randomUUID().toString();
        createIndexes(locksCollection);
    }

    @Override
    public boolean tryLock() {
        if (!acquireLock()) {
            return false;
        }

        logger.trace("Lock acquired: {}", this.ownerId);

        startRenewalScheduler();
        return true;
    }

    @Override
    public void releaseLock() {
        stopRenewalScheduler();

        final var result = locksCollection.deleteOne(
                Filters.and(
                        Filters.eq(LOCK_ID_FIELD, LOCK_ID),
                        Filters.eq(OWNER_ID_FIELD, this.ownerId)
                )
        );

        if (result.getDeletedCount() > 0) {
            logger.trace("Lock released by {}", this.ownerId);
        }
    }

    private boolean acquireLock() {
        final var expiresAt = Date.from(Instant.now().plusSeconds(LOCK_EXPIRATION_PERIOD));

        final var currentDate = new Date();
        final var lockDocument = new Document(LOCK_ID_FIELD, LOCK_ID)
                                        .append(EXPIRES_AT_FIELD, expiresAt)
                                        .append(OWNER_ID_FIELD, this.ownerId)
                                        .append(CREATED_AT_FIELD, currentDate);

        try {
            final var result = this.locksCollection.findOneAndUpdate(
                    Filters.and(
                            Filters.eq(LOCK_ID_FIELD, LOCK_ID),
                            Filters.or(
                                    Filters.eq(OWNER_ID_FIELD, this.ownerId),
                                    Filters.lte(EXPIRES_AT_FIELD, currentDate)
                            )
                    ),
                    Updates.combine(
                            Updates.set(OWNER_ID_FIELD, this.ownerId),
                            Updates.set(EXPIRES_AT_FIELD, expiresAt),
                            Updates.set(CREATED_AT_FIELD, currentDate)
                    ),
                    new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)
            );

            return result != null && this.ownerId.equals(result.getString(OWNER_ID_FIELD));
        } catch (Exception e) {
            logger.debug("Unable to acquire lock by server %s".formatted(this.ownerId), e);
            return false;
        }
    }

    private void startRenewalScheduler() {
        stopRenewalScheduler();
        this.renewalTask = this.lockingPool.scheduleAtFixedRate(this::renewLock, 10, 10, TimeUnit.SECONDS);
    }

    private void stopRenewalScheduler() {
        if (this.renewalTask != null) {
            this.renewalTask.cancel(true);
        }
    }

    private void renewLock() {
        final var newExpiresAt = Date.from(Instant.now().plusSeconds(LOCK_EXPIRATION_PERIOD));
        final var result = this.locksCollection.findOneAndUpdate(
                Filters.and(
                        Filters.eq(LOCK_ID_FIELD, LOCK_ID),
                        Filters.eq(OWNER_ID_FIELD, ownerId)
                ),
                Updates.set(EXPIRES_AT_FIELD, newExpiresAt),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
        );

        if (result == null) {
            stopRenewalScheduler();
        }
    }

    private void createIndexes(final MongoCollection<Document> locksCollection) {
        try {
            locksCollection.createIndex(new Document(LOCK_ID_FIELD, 1), new IndexOptions().name("idx_uid").unique(true));
            locksCollection.createIndex(new Document(EXPIRES_AT_FIELD, 1), new IndexOptions().name("idx_exp").expireAfter(0L, TimeUnit.SECONDS));
        } catch (MongoWriteException ignored) {
        }
    }
}
