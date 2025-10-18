package ru.joke.kdlq.mongo.internal;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class KDLQMongoGlobalDistributedLockService implements KDLQGlobalDistributedLockService {

    private static final String LOCK_ID = KDLQMongoGlobalDistributedLockService.class.getSimpleName();

    private final MongoCollection<Document> locksCollection;
    private final ScheduledExecutorService lockingPool;
    private final String ownerId;
    private volatile Future<?> renewalTask;

    private static final String LOCK_ID_FIELD = "_id";
    private static final String EXPIRES_AT_FIELD = "expiresAt";
    private static final String CREATED_AT_FIELD = "createdAt";
    private static final String OWNER_ID_FIELD = "owner";

    public KDLQMongoGlobalDistributedLockService(
            final MongoCollection<Document> locksCollection,
            final ScheduledExecutorService lockingPool
    ) {
        this.locksCollection = locksCollection;
        this.lockingPool = lockingPool;
        this.ownerId = UUID.randomUUID().toString();

        createIndexes(locksCollection);
        lockingPool.schedule(() -> {

        }, 5, TimeUnit.SECONDS);
    }

    @Override
    public boolean tryLock() {
        if (!acquireLock()) {
            return false;
        }

        startRenewalScheduler();
        return true;

    }

    @Override
    public void releaseLock() {
        stopRenewalScheduler();

        DeleteResult result = locksCollection.deleteOne(
                Filters.and(
                        Filters.eq(LOCK_ID_FIELD, LOCK_ID),
                        Filters.eq(OWNER_ID_FIELD, ownerId)
                )
        );
    }

    private boolean acquireLock() {
        Date expiresAt = Date.from(Instant.now().plusSeconds(60 * 60));

        Document lockDocument = new Document(LOCK_ID_FIELD, LOCK_ID)
                .append(EXPIRES_AT_FIELD, expiresAt)
                .append(OWNER_ID_FIELD, this.ownerId)
                .append(CREATED_AT_FIELD, new Date());

        try {
            Document result = locksCollection.findOneAndUpdate(
                    Filters.and(
                            Filters.eq(LOCK_ID_FIELD, LOCK_ID),
                            Filters.or(
                                    Filters.eq(OWNER_ID_FIELD, ownerId),
                                    Filters.lte(EXPIRES_AT_FIELD, new Date())
                            )
                    ),
                    Updates.combine(
                            Updates.set(OWNER_ID_FIELD, this.ownerId),
                            Updates.set(EXPIRES_AT_FIELD, expiresAt),
                            Updates.set(CREATED_AT_FIELD, new Date())
                    ),
                    new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)
            );

            return result != null && ownerId.equals(result.getString(OWNER_ID_FIELD));
        } catch (Exception e) {
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
        Date newExpiresAt = Date.from(Instant.now().plusSeconds(60));
        Document result = locksCollection.findOneAndUpdate(
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
