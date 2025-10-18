package ru.joke.kdlq.spi;

public interface KDLQGlobalDistributedLockService {

    boolean tryLock();

    void releaseLock();
}
