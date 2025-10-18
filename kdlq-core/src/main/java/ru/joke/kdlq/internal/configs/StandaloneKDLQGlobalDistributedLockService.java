package ru.joke.kdlq.internal.configs;

import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class StandaloneKDLQGlobalDistributedLockService implements KDLQGlobalDistributedLockService {

    private final Lock lock = new ReentrantLock();

    @Override
    public boolean tryLock() {
        return this.lock.tryLock();
    }

    @Override
    public void releaseLock() {
        this.lock.unlock();
    }
}
