package ru.joke.kdlq.internal.configs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.KDLQGlobalConfigurationFactory;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DefaultKDLQGlobalConfigurationFactoryTest {

    private KDLQGlobalConfigurationFactory factory;
    private ScheduledExecutorService redeliveryDispatcherPool;
    private ExecutorService redeliveryPool;
    private KDLQRedeliveryStorage redeliveryStorage;
    private KDLQGlobalDistributedLockService distributedLockService;

    @BeforeEach
    void setUp() {
        this.factory = new DefaultKDLQGlobalConfigurationFactory();
        this.redeliveryPool = mock(ExecutorService.class);
        this.redeliveryDispatcherPool = mock(ScheduledExecutorService.class);
        this.redeliveryStorage = mock(KDLQRedeliveryStorage.class);
        this.distributedLockService = mock(KDLQGlobalDistributedLockService.class);
    }

    @Test
    void testCreateStatelessStandaloneConfigurationWithProvidedPools() {
        final var expectedDelay = 1000L;
        final var config = factory.createStatelessStandaloneConfiguration(
                this.redeliveryDispatcherPool,
                this.redeliveryPool,
                expectedDelay
        );

        assertNotNull(config, "Config must be not null");
        assertSame(this.redeliveryDispatcherPool, config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be equal");
        assertSame(this.redeliveryPool, config.redeliveryPool(), "Redelivery pool must be equal");

        assertEquals(expectedDelay, config.redeliveryDispatcherTaskDelay(), "Redelivery dispatcher task delay must be equal");
        assertNotNull(config.redeliveryStorage(), "Redelivery storage must be not null");
        assertNotNull(config.distributedLockService(), "Distributed lock service must be not null");
    }

    @Test
    void testCreateStatelessStandaloneConfigurationWithNullPools() {
        final var expectedDelay = 1000L;
        final var config = factory.createStatelessStandaloneConfiguration(
                null,
                null,
                expectedDelay
        );

        assertNotNull(config, "Config must be not null");

        assertEquals(expectedDelay, config.redeliveryDispatcherTaskDelay(), "Redelivery dispatcher task delay must be equal");
        assertNotNull(config.redeliveryStorage(), "Redelivery storage must be not null");
        assertNotNull(config.distributedLockService(), "Distributed lock service must be not null");
        assertNotNull(config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be not null");
        assertNotNull(config.redeliveryPool(), "Redelivery pool must be not null");
    }

    @Test
    void testCreateStatelessStandaloneConfigurationWithoutProvidedPools() {
        final var expectedDelay = 1000L;
        final var config = factory.createStatelessStandaloneConfiguration(expectedDelay);

        assertNotNull(config, "Config must be not null");

        assertEquals(expectedDelay, config.redeliveryDispatcherTaskDelay(), "Redelivery dispatcher task delay must be equal");
        assertNotNull(config.redeliveryStorage(), "Redelivery storage must be not null");
        assertNotNull(config.distributedLockService(), "Distributed lock service must be not null");
        assertNotNull(config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be not null");
        assertNotNull(config.redeliveryPool(), "Redelivery pool must be not null");
    }

    @Test
    void testCreateStandaloneConfigurationWithProvidedPoolsAndRedeliveryStorage() {
        final var expectedDelay = 1000L;
        final var config = factory.createStandaloneConfiguration(
                this.redeliveryDispatcherPool,
                this.redeliveryPool,
                expectedDelay,
                this.redeliveryStorage
        );

        assertNotNull(config, "Config must be not null");
        assertSame(this.redeliveryDispatcherPool, config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be equal");
        assertSame(this.redeliveryPool, config.redeliveryPool(), "Redelivery pool must be equal");

        assertEquals(expectedDelay, config.redeliveryDispatcherTaskDelay(), "Redelivery dispatcher task delay must be equal");
        assertSame(this.redeliveryStorage, config.redeliveryStorage(), "Redelivery storage must be same");
        assertNotNull(config.distributedLockService(), "Distributed lock service must be not null");
    }

    @Test
    void testCreateStandaloneConfigurationWithNullPoolsAndRedeliveryStorage() {
        final var expectedDelay = 1000L;
        final var config = factory.createStandaloneConfiguration(
                null,
                null,
                expectedDelay,
                this.redeliveryStorage
        );

        assertNotNull(config, "Config must be not null");
        assertNotNull(config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be not null");
        assertNotNull(config.redeliveryPool(), "Redelivery pool must be not null");

        assertEquals(expectedDelay, config.redeliveryDispatcherTaskDelay(), "Redelivery dispatcher task delay must be equal");
        assertSame(this.redeliveryStorage, config.redeliveryStorage(), "Redelivery storage must be same");
        assertNotNull(config.distributedLockService(), "Distributed lock service must be not null");
    }

    @Test
    void testCreateStandaloneConfigurationWithProvidedRedeliveryStorageAndWithoutPools() {
        final var expectedDelay = 1000L;
        final var config = factory.createStandaloneConfiguration(
                expectedDelay,
                this.redeliveryStorage
        );

        assertNotNull(config, "Config must be not null");
        assertNotNull(config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be not null");
        assertNotNull(config.redeliveryPool(), "Redelivery pool must be not null");

        assertEquals(expectedDelay, config.redeliveryDispatcherTaskDelay(), "Redelivery dispatcher task delay must be equal");
        assertSame(this.redeliveryStorage, config.redeliveryStorage(), "Redelivery storage must be same");
        assertNotNull(config.distributedLockService(), "Distributed lock service must be not null");
    }

    @Test
   void testCreateCustomConfigurationWithAllParameters() {
        final var expectedDelay = 6000L;
        final var config = this.factory.createCustomConfiguration(
                this.redeliveryDispatcherPool,
                this.redeliveryPool,
                expectedDelay,
                this.distributedLockService,
                this.redeliveryStorage
        );

        assertNotNull(config, "Config must be not null");
        assertSame(this.redeliveryDispatcherPool, config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be equal");
        assertSame(this.redeliveryPool, config.redeliveryPool(), "Redelivery pool must be equal");

        assertEquals(expectedDelay, config.redeliveryDispatcherTaskDelay(), "Redelivery dispatcher task delay must be equal");
        assertSame(this.redeliveryStorage, config.redeliveryStorage(), "Redelivery storage must be same");
        assertSame(this.distributedLockService, config.distributedLockService(), "Distributed lock service must be same");
    }

    @Test
    void testCreateCustomConfigurationWithNullPoolsAndOtherParameters() {
        final var expectedDelay = 6500L;
        final var config = factory.createCustomConfiguration(
                null,
                null,
                expectedDelay,
                this.distributedLockService,
                this.redeliveryStorage
        );

        assertNotNull(config, "Config must be not null");
        assertNotNull(config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be not null");
        assertNotNull(config.redeliveryPool(), "Redelivery pool must be not null");

        assertEquals(expectedDelay, config.redeliveryDispatcherTaskDelay(), "Redelivery dispatcher task delay must be equal");
        assertSame(this.redeliveryStorage, config.redeliveryStorage(), "Redelivery storage must be same");
        assertSame(this.distributedLockService, config.distributedLockService(), "Distributed lock service must be same");
    }

    @Test
    void testCreateCustomConfigurationWithOnlyRequiredParameters() {
        final var expectedDelay = 7000L;
        final var config = factory.createCustomConfiguration(
                expectedDelay,
                this.distributedLockService,
                this.redeliveryStorage
        );

        assertNotNull(config, "Config must be not null");
        assertNotNull(config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be not null");
        assertNotNull(config.redeliveryPool(), "Redelivery pool must be not null");

        assertEquals(expectedDelay, config.redeliveryDispatcherTaskDelay(), "Redelivery dispatcher task delay must be equal");
        assertSame(this.redeliveryStorage, config.redeliveryStorage(), "Redelivery storage must be same");
        assertSame(this.distributedLockService, config.distributedLockService(), "Distributed lock service must be same");
    }
}
