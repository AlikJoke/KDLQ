package ru.joke.kdlq.internal.util;

import ru.joke.kdlq.KDLQException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread factory for scheduled executor service that creates daemon virtual threads.
 *
 * @author Alik
 */
@ThreadSafe
public final class DaemonVirtualThreadFactory implements ThreadFactory {

    private static final String THREAD_LABEL = "-thread-";

    private final AtomicInteger threadNumber = new AtomicInteger(0);
    private final String namePrefix;

    /**
     * Constructs the daemon thread factory with threads whose names start with a given prefix.
     *
     * @param prefix thread names prefix; cannot be {@code null}.
     */
    public DaemonVirtualThreadFactory(@Nonnull final String prefix) {
        this.namePrefix =
                Args.requireNotEmpty(
                        prefix,
                        () -> new KDLQException("Thread prefix must be not empty")
                ) + THREAD_LABEL;
    }

    @Override
    @Nonnull
    public Thread newThread(@Nonnull final Runnable runnable) {
        final var t = Thread.ofVirtual().unstarted(runnable);
        t.setName(this.namePrefix + this.threadNumber.getAndIncrement());
        t.setDaemon(true);
        return t;
    }
}
