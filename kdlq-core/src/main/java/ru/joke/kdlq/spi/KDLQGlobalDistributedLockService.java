package ru.joke.kdlq.spi;

import ru.joke.kdlq.KDLQGlobalConfiguration;

/**
 * A distributed lock service used to synchronize the initiation of message redelivery tasks
 * in a distributed system.<br>
 * When using KDLQ in a distributed system, the implementation of this
 * service must support synchronization between different nodes in the cluster.<br>
 * For a standalone system, a local lock implementation is sufficient, such as one based
 * on {@link java.util.concurrent.locks.Lock} or other JVM-level synchronization primitives.
 * @implSpec Implementations must be thread-safe.
 *
 * @author Alik
 * @see KDLQGlobalConfiguration#distributedLockService()
 * @see ru.joke.kdlq.KDLQGlobalConfigurationFactory
 */
public interface KDLQGlobalDistributedLockService {

    /**
     * Attempts to acquire a global distributed lock.
     * @implSpec Implementation must ensure that only one owner can hold the lock at any given time.
     *
     * @return {@code false} if the lock is already acquired;
     *         {@code true} if lock was successfully acquired by this method call.
     * @see #releaseLock()
     */
    boolean tryLock();

    /**
     * Releases the global distributed lock that was previously acquired by this KDLQ instance.
     *
     * @see #releaseLock()
     */
    void releaseLock();
}
