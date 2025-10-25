package ru.joke.kdlq.internal.util;

import org.junit.jupiter.api.Test;
import ru.joke.kdlq.KDLQException;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DaemonVirtualThreadFactoryTest {

    @Test
    void testFactoryConstructorValidation() {
        assertThrows(KDLQException.class, () -> new DaemonVirtualThreadFactory(""));
    }

    @Test
    void testThreadsCreation() throws InterruptedException {
        final var prefix = "test";
        final var factory = new DaemonVirtualThreadFactory("test");

        final var counter1 = new AtomicInteger();
        final var thread1 = factory.newThread(() -> counter1.incrementAndGet());
        makeThreadChecks(thread1, prefix, counter1);

        final var counter2 = new AtomicInteger();
        final var thread2 = factory.newThread(() -> counter2.incrementAndGet());
        makeThreadChecks(thread2, prefix, counter2);

        assertNotEquals(thread1.getName(), thread2.getName(), "Thread names must be not equal");
    }

    private void makeThreadChecks(final Thread thread, final String prefix, final AtomicInteger counter) throws InterruptedException {
        assertTrue(thread.isDaemon(), "Thread must be daemon");
        assertNotNull(thread.getName(), "Thread name must be not null");
        assertNotNull(thread.getName().startsWith(prefix), "Thread name must begin with provided prefix");
        assertTrue(thread.isVirtual(), "Thread must be virtual");

        thread.start();
        thread.join();

        assertEquals(1, counter.get(), "Counter value must be equal after thread execution");
    }
}
