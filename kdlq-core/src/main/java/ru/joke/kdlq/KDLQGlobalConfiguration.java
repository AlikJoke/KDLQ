package ru.joke.kdlq;

import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import java.util.concurrent.ScheduledExecutorService;

public interface KDLQGlobalConfiguration {

    ScheduledExecutorService redeliveryPool();

    long redeliveryTaskDelay();

    KDLQGlobalDistributedLockService distributedLockService();

    KDLQRedeliveryStorage redeliveryStorage();
}
