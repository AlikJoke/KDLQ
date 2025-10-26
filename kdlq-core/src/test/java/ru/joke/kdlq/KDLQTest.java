package ru.joke.kdlq;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import ru.joke.kdlq.internal.redelivery.RedeliveryTask;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class KDLQTest {

    private static final String CONFIG_ID = "1";

    private KDLQGlobalConfiguration globalConfiguration;
    private ScheduledExecutorService redeliveryDispatcherPool;
    private KDLQConfiguration configuration;

    @BeforeEach
    void setUp() {
        this.redeliveryDispatcherPool = mock(ScheduledExecutorService.class);

        final Map<String, Object> producerProperties = Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
        );
        this.configuration = mock(ImmutableKDLQConfiguration.class);
        when(this.configuration.producerProperties()).thenReturn(producerProperties);
        when(this.configuration.producerId()).thenReturn(CONFIG_ID);

        this.globalConfiguration =
                KDLQGlobalConfigurationFactory.getInstance().createCustomConfiguration(
                        this.redeliveryDispatcherPool,
                        null,
                        100L,
                        mock(KDLQGlobalDistributedLockService.class),
                        mock(KDLQRedeliveryStorage.class)
                );

        when(this.configuration.id()).thenReturn(CONFIG_ID);
    }

    @AfterEach
    void tearDown() {
        try {
            KDLQ.shutdown();
        } catch (Exception ignored) {
        }
    }

    @Test
    void testKDLQSuccessInitializationWithProvidedConfig() {
        KDLQ.initialize(globalConfiguration);
        verify(redeliveryDispatcherPool).schedule(
                any(RedeliveryTask.class),
                eq(this.globalConfiguration.redeliveryDispatcherTaskDelay()),
                eq(TimeUnit.MILLISECONDS)
        );
    }

    @Test
    void testKDLQSuccessInitializationWithoutProvidedConfig() {
        KDLQ.initialize(null);
        verify(redeliveryDispatcherPool, never()).schedule(
                any(RedeliveryTask.class),
                eq(this.globalConfiguration.redeliveryDispatcherTaskDelay()),
                eq(TimeUnit.MILLISECONDS)
        );
    }

    @Test
    void testKDLQInitializationWhenAlreadyInitialized() {
        KDLQ.initialize(globalConfiguration);
        assertThrows(KDLQLifecycleException.class, () -> KDLQ.initialize(globalConfiguration));
    }

    @Test
    void testShutdownOfNotInitializedKDLQ() {
        assertThrows(KDLQLifecycleException.class, KDLQ::shutdown);
    }

    @Test
    void testSuccessKDLQShutdown() {
        KDLQ.initialize();
        KDLQ.registerConfiguration(this.configuration);
        KDLQ.registerConsumer("1", this.configuration, mock(KDLQMessageProcessor.class));
        KDLQ.shutdown();

        KDLQ.initialize();
        final boolean unregisteredConfig = KDLQ.unregisterConfiguration(this.configuration);
        assertFalse(unregisteredConfig, "Configuration must not present in KDLQ after shutdown");

        final boolean unregisteredConsumer = KDLQ.unregisterConsumer("1");
        assertFalse(unregisteredConsumer, "Consumer must not present in KDLQ after shutdown");
    }

    @Test
    void testSuccessRegisterConsumerWhenConfigAlreadyRegistered() {
        KDLQ.initialize();

        KDLQ.registerConfiguration(this.configuration);

        final var consumerId = "c1";
        final var consumer = KDLQ.registerConsumer(
                consumerId,
                this.configuration.id(),
                s -> KDLQMessageProcessor.ProcessingStatus.OK
        );

        assertNotNull(consumer, "Created consumer must be not null");
        assertEquals(consumerId, consumer.id(), "Consumer id must be equal");
    }

    @Test
    void testSuccessRegisterConsumerWhenConfigIsNotRegisteredYet() {
        KDLQ.initialize();

        final var consumerId = "c1";
        final var consumer = KDLQ.registerConsumer(
                consumerId,
                this.configuration,
                s -> KDLQMessageProcessor.ProcessingStatus.OK
        );

        assertNotNull(consumer, "Created consumer must be not null");
        assertEquals(consumerId, consumer.id(), "Consumer id must be equal");

        final boolean registered = KDLQ.registerConfiguration(this.configuration);
        assertFalse(registered, "Configuration must be registered via consumer creation");
    }

    @Test
    void testRegisterConsumerWhenAlreadyRegistered() {
        KDLQ.initialize();

        final var consumerId = "c1";
        KDLQ.registerConsumer(consumerId, this.configuration, s -> KDLQMessageProcessor.ProcessingStatus.OK);
        assertThrows(
                KDLQException.class,
                () -> KDLQ.registerConsumer(consumerId, this.configuration, s -> KDLQMessageProcessor.ProcessingStatus.OK)
        );
    }

    @Test
    void testRegisterConsumerWhenKDLQIsNotInitialized() {
        assertThrows(
                KDLQLifecycleException.class,
                () -> KDLQ.registerConsumer("1", this.configuration, mock(KDLQMessageProcessor.class))
        );
    }

    @Test
    void testRegisterConsumerWithNullId() {
        KDLQ.initialize();
        assertThrows(
                KDLQException.class,
                () -> KDLQ.registerConsumer(null, this.configuration, mock(KDLQMessageProcessor.class))
        );
    }

    @Test
    void testRegisterConsumerWithNullConfig() {
        KDLQ.initialize();
        assertThrows(
                KDLQException.class,
                () -> KDLQ.registerConsumer("1", (KDLQConfiguration) null, mock(KDLQMessageProcessor.class))
        );
    }

    @Test
    void registerConsumer_nullProcessor_throwsException() {
        KDLQ.initialize();
        assertThrows(
                KDLQException.class,
                () -> KDLQ.registerConsumer("1", this.configuration, null)
        );
    }

    @Test
    void testRegisterConsumerWithConfigIdWhenKDLQIsNotInitialized() {
        assertThrows(
                KDLQLifecycleException.class,
                () -> KDLQ.registerConsumer("1", CONFIG_ID, mock(KDLQMessageProcessor.class))
        );
    }

    @Test
    void testRegisterConsumerWhenConfigIdNotFound() {
        KDLQ.initialize();
        assertThrows(
                KDLQException.class,
                () -> KDLQ.registerConsumer("1", CONFIG_ID, mock(KDLQMessageProcessor.class))
        );
    }

    @Test
    void testSuccessUnregisterConsumer() {
        KDLQ.initialize();

        final var registeredConsumer = KDLQ.registerConsumer(
                "1",
                this.configuration,
                mock(KDLQMessageProcessor.class)
        );

        final boolean unregistered = KDLQ.unregisterConsumer(registeredConsumer);
        assertTrue(unregistered, "Consumer must be unregistered");
    }

    @Test
    void testSuccessUnregisterConsumerById() {
        KDLQ.initialize();

        final var consumerId = "1";
        KDLQ.registerConsumer(consumerId, this.configuration, mock(KDLQMessageProcessor.class));

        final boolean unregistered = KDLQ.unregisterConsumer(consumerId);
        assertTrue(unregistered, "Consumer must be unregistered");
    }


    @Test
    void testUnregisterNotExistentConsumer() {
        KDLQ.initialize();
        final boolean unregistered = KDLQ.unregisterConsumer("1");
        assertFalse(unregistered, "Consumer must be not registered");
    }

    @Test
    void testUnregisterConsumerWhenKDLQIsNotInitialized() {
        assertThrows(
                KDLQLifecycleException.class,
                () -> KDLQ.unregisterConsumer(mock(KDLQMessageConsumer.class))
        );
        assertThrows(
                KDLQLifecycleException.class,
                () -> KDLQ.unregisterConsumer("1")
        );
    }

    @Test
    void testUnregisterNullConsumer() {
        KDLQ.initialize();
        assertThrows(
                KDLQException.class,
                () -> KDLQ.unregisterConsumer((KDLQMessageConsumer<?, ?>) null)
        );
    }

    @Test
    void testUnregisterNullConsumerId() {
        KDLQ.initialize();
        assertThrows(
                KDLQException.class,
                () -> KDLQ.unregisterConsumer((String) null)
        );
    }

    @Test
    void testSuccessRegisterConfiguration() {
        KDLQ.initialize();
        final boolean registered = KDLQ.registerConfiguration(this.configuration);
        assertTrue(registered, "Configuration must be registered");
    }

    @Test
    void testRegisterConfigurationWhenAlreadyRegistered() {
        KDLQ.initialize();
        KDLQ.registerConfiguration(this.configuration);
        boolean registeredAgain = KDLQ.registerConfiguration(this.configuration);
        assertFalse(registeredAgain, "Configuration must be already registered");
    }

    @Test
    void testRegisterConfigurationWhenKDLQIsNotInitialized() {
        assertThrows(
                KDLQLifecycleException.class,
                () -> KDLQ.registerConfiguration(this.configuration)
        );
    }

    @Test
    void testRegisterConfigurationWithNullConfig() {
        KDLQ.initialize();
        assertThrows(
                KDLQException.class,
                () -> KDLQ.registerConfiguration(null)
        );
    }

    @Test
    void testSuccessUnregisterConfiguration() {
        KDLQ.initialize();
        KDLQ.registerConfiguration(this.configuration);
        final boolean unregistered = KDLQ.unregisterConfiguration(this.configuration);
        assertTrue(unregistered, "Configuration must be unregistered");
    }

    @Test
    void testSuccessUnregisterConfigurationById() {
        KDLQ.initialize();
        KDLQ.registerConfiguration(this.configuration);
        final boolean unregistered = KDLQ.unregisterConfiguration(CONFIG_ID);
        assertTrue(unregistered, "Configuration must be unregistered");
    }

    @Test
    void testSuccessRegisterConfigurationWithReplace() {
        KDLQ.initialize();

        KDLQ.registerConfiguration(this.configuration);
        final boolean replaced = KDLQ.registerConfiguration(this.configuration, true);

        assertTrue(replaced, "Configuration must be replaced");
    }

    @Test
    void testFailureRegisterConfigurationWithReplaceWhenExistRunningConsumer() {
        KDLQ.initialize();

        KDLQ.registerConfiguration(this.configuration);
        KDLQ.registerConsumer("1", this.configuration, mock(KDLQMessageProcessor.class));

        assertThrows(
                KDLQConfigurationLifecycleException.class,
                () -> KDLQ.registerConfiguration(this.configuration, true)
        );
    }

    @Test
    void testFailureUnregisterConfigurationWhenExistRunningConsumer() {
        KDLQ.initialize();

        KDLQ.registerConfiguration(this.configuration);
        KDLQ.registerConsumer("1", this.configuration, mock(KDLQMessageProcessor.class));

        assertThrows(
                KDLQConfigurationLifecycleException.class,
                () -> KDLQ.unregisterConfiguration(this.configuration)
        );

        assertThrows(
                KDLQConfigurationLifecycleException.class,
                () -> KDLQ.unregisterConfiguration(CONFIG_ID)
        );
    }

    @Test
    void testUnregisterNotRegisteredConfiguration() {
        KDLQ.initialize();
        final boolean unregistered = KDLQ.unregisterConfiguration("nonExistentConfig");
        assertFalse(unregistered, "Configuration must not be registered");
    }

    @Test
    void testUnregisterConfigurationWhenKDLQIsNotInitialized() {
        assertThrows(
                KDLQLifecycleException.class,
                () -> KDLQ.unregisterConfiguration(this.configuration)
        );
        assertThrows(
                KDLQLifecycleException.class,
                () -> KDLQ.unregisterConfiguration(CONFIG_ID)
        );
    }

    @Test
    void testUnregisterConfigurationWithNullConfig() {
        KDLQ.initialize();
        assertThrows(
                KDLQException.class,
                () -> KDLQ.unregisterConfiguration((KDLQConfiguration) null)
        );
    }

    @Test
    void testUnregisterConfigurationWithNullConfigId() {
        KDLQ.initialize();
        assertThrows(
                KDLQException.class,
                () -> KDLQ.unregisterConfiguration((String) null)
        );
    }
}
