package ru.joke.kdlq.internal.redelivery;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.KDLQGlobalConfiguration;
import ru.joke.kdlq.internal.configs.InternalKDLQConfigurationRegistry;
import ru.joke.kdlq.internal.configs.KDLQConfigurationRegistry;
import ru.joke.kdlq.internal.routers.headers.KDLQHeadersService;
import ru.joke.kdlq.internal.routers.producers.InternalKDLQMessageProducerFactory;
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducerFactory;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RedeliveryTaskTest {

    private KDLQGlobalDistributedLockService lockService;
    private KDLQRedeliveryStorage redeliveryStorage;
    private ScheduledExecutorService redeliveryDispatcherPool;
    private ScheduledFuture<?> dispatcherFuture;
    private KDLQGlobalConfiguration globalConfiguration;
    private KDLQConfigurationRegistry configurationRegistry;
    private KDLQMessageProducerFactory producerFactory;
    private RedeliveryTask task;

    @BeforeEach
    void setUp() {
        this.lockService = mock(KDLQGlobalDistributedLockService.class);
        this.redeliveryStorage = mock(KDLQRedeliveryStorage.class);
        this.redeliveryDispatcherPool = mock(ScheduledExecutorService.class);
        this.dispatcherFuture = mock(ScheduledFuture.class);

        this.producerFactory = mock(InternalKDLQMessageProducerFactory.class);
        this.configurationRegistry = mock(InternalKDLQConfigurationRegistry.class);

        this.globalConfiguration =
                KDLQGlobalConfiguration.builder()
                        .withRedeliveryDispatcherPool(this.redeliveryDispatcherPool)
                .build(this.lockService, this.redeliveryStorage);
        this.task = new RedeliveryTask(
                this.globalConfiguration,
                this.configurationRegistry,
                this.producerFactory,
                new KDLQHeadersService()
        );
    }

    @Test
    void testTaskSingleRunCycle() {
        when(redeliveryDispatcherPool.schedule(
                same(this.task),
                eq(this.globalConfiguration.redeliveryDispatcherTaskDelay()),
                eq(TimeUnit.MILLISECONDS))
        ).then(i -> dispatcherFuture);

        this.task.run();

        verifyNoInteractions(lockService);
        verifyNoInteractions(redeliveryStorage);
        verify(redeliveryDispatcherPool).schedule(
                same(this.task),
                eq(this.globalConfiguration.redeliveryDispatcherTaskDelay()),
                eq(TimeUnit.MILLISECONDS)
        );
    }

    @Test
    void testTaskTwoRunCycle() {
        when(lockService.tryLock()).thenReturn(true);
        when(redeliveryDispatcherPool.schedule(
                same(this.task),
                eq(this.globalConfiguration.redeliveryDispatcherTaskDelay()),
                eq(TimeUnit.MILLISECONDS))
        ).then(i -> dispatcherFuture);
        when(redeliveryStorage.findAllReadyToRedelivery(any(), anyLong(), anyInt())).thenReturn(Collections.emptyList());

        this.task.run();
        this.task.run();

        verify(lockService).tryLock();
        verify(redeliveryStorage).findAllReadyToRedelivery(any(), anyLong(), anyInt());
        verify(lockService).releaseLock();
        verify(redeliveryDispatcherPool, times(2)).schedule(
                same(this.task),
                eq(this.globalConfiguration.redeliveryDispatcherTaskDelay()),
                eq(TimeUnit.MILLISECONDS)
        );
    }

    @Test
    void testRunSchedulesNextRedeliveryOnLockFailure() {
        when(lockService.tryLock()).thenReturn(false);
        when(redeliveryDispatcherPool.schedule(
                same(this.task),
                eq(this.globalConfiguration.redeliveryDispatcherTaskDelay()),
                eq(TimeUnit.MILLISECONDS)
        )).then(i -> dispatcherFuture);

        this.task.run();
        this.task.run();

        verify(lockService).tryLock();
        verify(redeliveryStorage, never()).findAllReadyToRedelivery(any(), anyLong(), anyInt());
        verify(lockService, never()).releaseLock();
        verify(redeliveryDispatcherPool, times(2)).schedule(
                same(this.task),
                eq(this.globalConfiguration.redeliveryDispatcherTaskDelay()),
                eq(TimeUnit.MILLISECONDS)
        );
    }

    @Test
    void testRunHandlesExceptionAndSchedulesNextRedelivery() {
        when(lockService.tryLock()).thenReturn(true);
        when(redeliveryStorage.findAllReadyToRedelivery(any(), anyLong(), anyInt())).thenThrow(new RuntimeException("Some exception"));
        when(redeliveryDispatcherPool.schedule(
                same(this.task),
                eq(this.globalConfiguration.redeliveryDispatcherTaskDelay()),
                eq(TimeUnit.MILLISECONDS)
        )).then(i -> dispatcherFuture);

        this.task.run();
        this.task.run();

        verify(lockService).tryLock();
        verify(redeliveryStorage).findAllReadyToRedelivery(any(), anyLong(), anyInt());
        verify(lockService).releaseLock();
        verify(redeliveryDispatcherPool, times(2)).schedule(
                same(this.task),
                eq(this.globalConfiguration.redeliveryDispatcherTaskDelay()),
                eq(TimeUnit.MILLISECONDS)
        );
    }

    @Test
    void testRunDoesNotScheduleNextRedeliveryIfDispatcherFutureIsCancelledDuringException() {
        when(lockService.tryLock()).thenReturn(true);
        when(redeliveryStorage.findAllReadyToRedelivery(any(), anyLong(), anyInt())).thenThrow(new RuntimeException("Test exception"));
        when(dispatcherFuture.isCancelled()).thenReturn(true);
        when(redeliveryDispatcherPool.schedule(
                same(this.task),
                eq(this.globalConfiguration.redeliveryDispatcherTaskDelay()),
                eq(TimeUnit.MILLISECONDS)
        )).then(i -> dispatcherFuture);

        this.task.run();
        this.task.run();

        verify(lockService).tryLock();
        verify(redeliveryStorage).findAllReadyToRedelivery(any(), anyLong(), anyInt());
        verify(lockService).releaseLock();
        verify(redeliveryDispatcherPool).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }
}
