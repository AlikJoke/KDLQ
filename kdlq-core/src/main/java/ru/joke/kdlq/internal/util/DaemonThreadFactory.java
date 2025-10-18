package ru.joke.kdlq.internal.util;

import javax.annotation.Nonnull;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class DaemonThreadFactory implements ThreadFactory {

    private static final String THREAD_LABEL = "-thread-";

    private final AtomicInteger threadNumber = new AtomicInteger(0);
    private final String namePrefix;

    public DaemonThreadFactory(final String prefix) {
        this.namePrefix = prefix + THREAD_LABEL;
    }

    @Override
    public Thread newThread(@Nonnull final Runnable runnable) {
        Thread t = new Thread(runnable, this.namePrefix + this.threadNumber.getAndIncrement());
        t.setDaemon(true);
        return t;
    }
}
