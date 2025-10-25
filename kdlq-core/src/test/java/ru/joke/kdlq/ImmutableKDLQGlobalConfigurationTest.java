package ru.joke.kdlq;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class ImmutableKDLQGlobalConfigurationTest {

    private static final long DEFAULT_DELAY = 1_000L;
    private static final long CUSTOM_DELAY = 5_000L;

    private ScheduledExecutorService redeliveryDispatcherPool;
    private ExecutorService redeliveryPool;
    private KDLQGlobalDistributedLockService distributedLockService;
    private KDLQRedeliveryStorage redeliveryStorage;

    @BeforeEach
    void setUp() {
        this.redeliveryPool = mock(ExecutorService.class);
        this.redeliveryDispatcherPool = mock(ScheduledExecutorService.class);
        this.distributedLockService = mock(KDLQGlobalDistributedLockService.class);
        this.redeliveryStorage = mock(KDLQRedeliveryStorage.class);
    }

    @Test
    void testCreationViaConstructorWithValidArguments() {
        final var config = new ImmutableKDLQGlobalConfiguration(
                this.redeliveryDispatcherPool,
                this.redeliveryPool,
                CUSTOM_DELAY,
                this.distributedLockService,
                this.redeliveryStorage
        );

        assertNotNull(config);
        assertEquals(this.redeliveryDispatcherPool, config.redeliveryDispatcherPool());
        assertEquals(this.redeliveryPool, config.redeliveryPool());
        assertEquals(CUSTOM_DELAY, config.redeliveryDispatcherTaskDelay());
        assertEquals(this.distributedLockService, config.distributedLockService());
        assertEquals(this.redeliveryStorage, config.redeliveryStorage());
    }

    @Test
    void testCreationViaConstructorWithNullRedeliveryDispatcherPool() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> new ImmutableKDLQGlobalConfiguration(
                        null,
                        this.redeliveryPool,
                        CUSTOM_DELAY,
                        this.distributedLockService,
                        this.redeliveryStorage
                )
        );
    }

    @Test
    void testCreationViaConstructorWithNullRedeliveryPool() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> new ImmutableKDLQGlobalConfiguration(
                        this.redeliveryDispatcherPool,
                        null,
                        CUSTOM_DELAY,
                        this.distributedLockService,
                        this.redeliveryStorage
                )
        );
    }

    @Test
    void testCreationViaConstructorWithNullRedeliveryStorage() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> new ImmutableKDLQGlobalConfiguration(
                        this.redeliveryDispatcherPool,
                        this.redeliveryPool,
                        CUSTOM_DELAY,
                        this.distributedLockService,
                        null
                )
        );
    }

    @Test
    void testCreationViaConstructorWithNullDistributedLockService() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> new ImmutableKDLQGlobalConfiguration(
                        this.redeliveryDispatcherPool,
                        this.redeliveryPool,
                        CUSTOM_DELAY,
                        null,
                        this.redeliveryStorage
                )
        );
    }

    @Test
    void testCreationViaConstructorWithZeroRedeliveryDispatcherTaskDelay() {
        for (int expectedDelay : new int[] { 0, -1 }) {
            assertThrows(
                    KDLQConfigurationException.class,
                    () -> new ImmutableKDLQGlobalConfiguration(
                            this.redeliveryDispatcherPool,
                            this.redeliveryPool,
                            expectedDelay,
                            this.distributedLockService,
                            this.redeliveryStorage
                    )
            );
        }
    }

    @Test
    void testBuilderReturnsNonNullObject() {
        assertNotNull(ImmutableKDLQGlobalConfiguration.builder(), "Builder must be not null");
    }

    @Test
    void testCreationViaBuilderWithoutRedeliveryTaskDelay() {
        final var config =
                ImmutableKDLQGlobalConfiguration.builder()
                                                    .withRedeliveryDispatcherPool(this.redeliveryDispatcherPool)
                                                    .withRedeliveryPool(this.redeliveryPool)
                                                .build(this.distributedLockService, this.redeliveryStorage);

        assertNotNull(config, "Created config must be not null");
        assertSame(this.distributedLockService, config.distributedLockService(), "Distributed lock service must be same");
        assertSame(this.redeliveryPool, config.redeliveryPool(), "Redelivery pool must be same");
        assertSame(this.redeliveryStorage, config.redeliveryStorage(), "Redelivery storage must be same");
        assertSame(this.redeliveryDispatcherPool, config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be same");
        assertEquals(DEFAULT_DELAY, config.redeliveryDispatcherTaskDelay(), "Default delay must be specified");
    }

    @Test
    void testCreationViaBuilderWithoutRedeliveryDispatcherPool() {
        final var config =
                ImmutableKDLQGlobalConfiguration.builder()
                                                    .withRedeliveryPool(this.redeliveryPool)
                                                .build(this.distributedLockService, this.redeliveryStorage);

        assertNotNull(config, "Created config must be not null");
        assertSame(this.distributedLockService, config.distributedLockService(), "Distributed lock service must be same");
        assertSame(this.redeliveryPool, config.redeliveryPool(), "Redelivery pool must be same");
        assertSame(this.redeliveryStorage, config.redeliveryStorage(), "Redelivery storage must be same");
        assertNotNull(config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be not null");
        assertEquals(DEFAULT_DELAY, config.redeliveryDispatcherTaskDelay(), "Default delay must be specified");
    }

    @Test
    void testCreationViaBuilderWithoutRedeliveryPool() {
        final var config =
                ImmutableKDLQGlobalConfiguration.builder()
                                                    .withRedeliveryDispatcherPool(this.redeliveryDispatcherPool)
                                                .build(this.distributedLockService, this.redeliveryStorage);

        assertNotNull(config, "Created config must be not null");
        assertSame(this.distributedLockService, config.distributedLockService(), "Distributed lock service must be same");
        assertSame(this.redeliveryDispatcherPool, config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be same");
        assertSame(this.redeliveryStorage, config.redeliveryStorage(), "Redelivery storage must be same");
        assertNotNull(config.redeliveryPool(), "Redelivery pool must be not null");
        assertEquals(DEFAULT_DELAY, config.redeliveryDispatcherTaskDelay(), "Default delay must be specified");
    }

    @Test
    void testCreationViaBuilderWithAllParameters() {
        final var config =
                ImmutableKDLQGlobalConfiguration.builder()
                                                    .withRedeliveryDispatcherPool(this.redeliveryDispatcherPool)
                                                    .withRedeliveryPool(this.redeliveryPool)
                                                    .withRedeliveryDispatcherTaskDelay(CUSTOM_DELAY)
                                                .build(this.distributedLockService, this.redeliveryStorage);

        assertNotNull(config, "Created config must be not null");
        assertSame(this.distributedLockService, config.distributedLockService(), "Distributed lock service must be same");
        assertSame(this.redeliveryDispatcherPool, config.redeliveryDispatcherPool(), "Redelivery dispatcher pool must be same");
        assertSame(this.redeliveryPool, config.redeliveryPool(), "Redelivery pool must be same");
        assertSame(this.redeliveryStorage, config.redeliveryStorage(), "Redelivery storage must be same");
        assertEquals(CUSTOM_DELAY, config.redeliveryDispatcherTaskDelay(), "Redelivery delay must be equal");
    }

    @Test
    void testCreationViaBuilderWithNullLockService() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> ImmutableKDLQGlobalConfiguration.builder()
                                .withRedeliveryDispatcherPool(this.redeliveryDispatcherPool)
                                .withRedeliveryPool(this.redeliveryPool)
                                .withRedeliveryDispatcherTaskDelay(CUSTOM_DELAY)
                        .build(null, this.redeliveryStorage)
        );
    }

    @Test
    void testCreationViaBuilderWithNullRedeliveryStorage() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> ImmutableKDLQGlobalConfiguration.builder()
                            .withRedeliveryDispatcherPool(this.redeliveryDispatcherPool)
                            .withRedeliveryPool(this.redeliveryPool)
                            .withRedeliveryDispatcherTaskDelay(CUSTOM_DELAY)
                        .build(this.distributedLockService, null)
        );
    }

    @Test
    void testCreationViaBuilderWithNegativeRedeliveryTaskDelay() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> ImmutableKDLQGlobalConfiguration.builder()
                            .withRedeliveryDispatcherPool(this.redeliveryDispatcherPool)
                            .withRedeliveryPool(this.redeliveryPool)
                            .withRedeliveryDispatcherTaskDelay(-1)
                        .build(this.distributedLockService, this.redeliveryStorage)
        );
    }
}
