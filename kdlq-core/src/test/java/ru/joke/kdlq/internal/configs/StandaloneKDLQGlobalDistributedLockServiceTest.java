package ru.joke.kdlq.internal.configs;

import org.junit.jupiter.api.Test;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class StandaloneKDLQGlobalDistributedLockServiceTest {

    private final KDLQGlobalDistributedLockService lockService = new StandaloneKDLQGlobalDistributedLockService();

    @Test
    void testWhenLockIsNotHoldButReleaseIsCalled() {
        assertThrows(IllegalMonitorStateException.class, () -> lockService.releaseLock());
    }

    @Test
    void testLockWhenAlreadyLockedInSameThread() {
        assertTrue(lockService.tryLock(), "Should be acquired");
        assertTrue(lockService.tryLock(), "Should be acquired");
    }

    @Test
    void testLockInOtherThreadWhenAlreadyLockedInCurrentThread() throws InterruptedException {
        assertTrue(lockService.tryLock(), "Should be acquired");

        final AtomicReference lockedInOtherThread = new AtomicReference<>();

        final var thread = new Thread(() -> lockedInOtherThread.set(this.lockService.tryLock()));
        thread.start();
        thread.join();

        assertEquals(Boolean.FALSE, lockedInOtherThread.get(), "Should not be locked");
    }

    @Test
    void testLockInCurrentThreadWhenAlreadyLockedInOtherThread() throws InterruptedException {
        final AtomicReference lockedInOtherThread = new AtomicReference<>();

        final var thread = new Thread(() -> lockedInOtherThread.set(this.lockService.tryLock()));
        thread.start();
        thread.join();

        assertEquals(Boolean.TRUE, lockedInOtherThread.get(), "Should be locked");

        assertFalse(lockService.tryLock(), "Should not be acquired");
    }

    @Test
    void testLockInCurrentThreadWhenLockedAndReleasedInOtherThread() throws InterruptedException {
        final AtomicReference lockedInOtherThread = new AtomicReference<>();

        final var thread = new Thread(() -> {
            lockedInOtherThread.set(this.lockService.tryLock());
            this.lockService.releaseLock();
        });
        thread.start();
        thread.join();

        assertEquals(Boolean.TRUE, lockedInOtherThread.get(), "Should be locked");

        assertTrue(lockService.tryLock(), "Should be acquired");
    }

    @Test
    void testLockAndReleaseInCurrentThread() {
        assertTrue(lockService.tryLock(), "Should be acquired");
        lockService.releaseLock();
        assertTrue(lockService.tryLock(), "Should be acquired");
    }
}
