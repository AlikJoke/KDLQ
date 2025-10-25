package ru.joke.kdlq;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class ImmutableKDLQConfigurationTest {

    private static final String TEST_QUEUE_1 = "test1";
    private static final String TEST_QUEUE_2 = "test2";

    private static final Set<String> bootstrapServers = Set.of("localhost:9092");
    private static final Map<String, Object> producerProperties = Map.of("p1", "v1");
    private static final KDLQConfiguration.DLQ dlq = new ImmutableKDLQConfiguration.DLQ("dlq-topic", 3);

    @Test
    void testSuccessfulCreation() {
        final var dlqName = "dlq-topic";
        final int maxKills = 3;

        final var dlq = new ImmutableKDLQConfiguration.DLQ(dlqName, 3);
        final KDLQMessageLifecycleListener listener = mock(KDLQMessageLifecycleListener.class);

        final var config = ImmutableKDLQConfiguration.builder()
                                                        .withId("test-config")
                                                        .withLifecycleListener(listener)
                                                        .addInformationalHeaders(true)
                                                      .build(bootstrapServers, producerProperties, dlq);

        makeConfigurationChecks(
                config,
                bootstrapServers,
                dlqName,
                null,
                maxKills,
                5,
                producerProperties
        );
        assertEquals(1, config.lifecycleListeners().size(), "Listeners count must be equal");
        assertTrue(config.addOptionalInformationalHeaders(), "Informational headers must be added");
    }

    @Test
    void testBuilderCreationWithInvalidBootstrapServers() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> ImmutableKDLQConfiguration.builder().build(null, producerProperties, dlq)
        );
        assertThrows(
                KDLQConfigurationException.class,
                () -> ImmutableKDLQConfiguration.builder().build(Collections.emptySet(), producerProperties, dlq)
        );
    }

    @Test
    void testBuilderCreationWithInvalidProducerProperties() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> ImmutableKDLQConfiguration.builder().build(bootstrapServers, null, dlq)
        );
        assertThrows(
                KDLQConfigurationException.class,
                () -> ImmutableKDLQConfiguration.builder().build(bootstrapServers, Collections.emptyMap(), dlq)
        );
    }

    @Test
    void testBuilderCreationWithNullDLQ() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> ImmutableKDLQConfiguration.builder().build(bootstrapServers, producerProperties, null)
        );
    }

    @Test
    void testBuilderWithNullLifecycleListener() {
        assertThrows(
                NullPointerException.class,
                () -> ImmutableKDLQConfiguration.builder().withLifecycleListener(null)
        );
    }

    @Test
    void testBuilderWithNullLifecycleListenersCollection() {
        assertThrows(
                NullPointerException.class,
                () -> ImmutableKDLQConfiguration.builder().withLifecycleListeners(null)
        );
    }

    @Test
    void testDefaultRedeliveryConfiguration() {
        final var config = ImmutableKDLQConfiguration.builder().build(bootstrapServers, producerProperties, dlq);

        assertEquals(new ImmutableKDLQConfiguration.Redelivery(null, 5, 1.5, 0, 0), config.redelivery(), "Redelivery config must be equal");
    }

    @Test
    void testOverrideRedeliveryConfiguration() {
        final var redelivery =
                ImmutableKDLQConfiguration.Redelivery.builder()
                            .withRedeliveryQueueName("redelivery-topic")
                            .withMaxRedeliveryAttemptsBeforeKill(10)
                            .withRedeliveryDelayMultiplier(2.0)
                            .withRedeliveryDelay(1000)
                            .withMaxRedeliveryDelay(5000)
                        .build();

        final var config = ImmutableKDLQConfiguration.builder()
                    .withRedelivery(redelivery)
                .build(bootstrapServers, producerProperties, dlq);

        assertSame(redelivery, config.redelivery(), "Redelivery config must be equal");
    }

    @Test
    void testDefaultIdGeneration() {
        final var config =
                ImmutableKDLQConfiguration.builder().build(bootstrapServers, producerProperties, dlq);

        assertNotNull(config.id(), "Config id must be not null");
        try {
            UUID.fromString(config.id());
        } catch (IllegalArgumentException e) {
            fail("Id is not a valid UUID: " + config.id());
        }
    }

    @Test
    void testEqualsAndHashCode() {
        final var config1 =
                ImmutableKDLQConfiguration.builder()
                    .withId("test-config")
                .build(bootstrapServers, producerProperties, dlq);

        final var bootstrapServers2 = Set.of("localhost:9093");
        final Map<String, Object> producerProperties2 = Map.of("p2", "v2");
        final var dlq2 = new ImmutableKDLQConfiguration.DLQ("dlq-topic2", 5);

        final var config2 = ImmutableKDLQConfiguration.builder()
                    .withId("test-config")
                .build(bootstrapServers2, producerProperties2, dlq2);

        final var config3 = ImmutableKDLQConfiguration.builder()
                    .withId("another-config")
                .build(bootstrapServers2, producerProperties2, dlq2);

        assertEquals(config1, config2, "Configs with equal id must be equal");
        assertEquals(config1.hashCode(), config2.hashCode(), "Hash code of configs with equal id must be equal");
        assertNotEquals(config1, config3, "Configs with different id must be not equals");
        assertNotEquals(config2, config3, "Configs with different id must be not equals");
    }

    @Test
    void testDLQSuccessfulCreation() {
        final var queue = "test";
        final int maxKills = 3;
        final var dlq = new ImmutableKDLQConfiguration.DLQ(queue, maxKills);
        assertEquals(queue, dlq.deadLetterQueueName(), "Queue name must be equal");
        assertEquals(maxKills, dlq.maxKills(), "Max kills must be equal");
    }

    @Test
    void testDLQCreationWithInvalidDeadLetterQueueName() {
        for (var invalidName : new String[] { "", null }) {
            assertThrows(KDLQConfigurationException.class, () -> new ImmutableKDLQConfiguration.DLQ(invalidName, 3));
        }
    }

    @Test
    void testDLQBuilderSuccessfulBuild() {
        final var queueName = "test";
        final var dlq =
                ImmutableKDLQConfiguration.DLQ.builder()
                            .withMaxKills(10)
                        .build(queueName);

        assertNotNull(dlq, "DLQ config must be not null");
        assertEquals(queueName, dlq.deadLetterQueueName(), "Queue name must be equal");
        assertEquals(10, dlq.maxKills(), "Max kills must be equal");
    }

    @Test
    void testRedeliverySuccessfulCreation() {
        final int maxRedeliveryDelay = 5000;
        final int redeliveryDelay = 50;
        final double multiplier = 2.0;
        final int maxRedeliveryAttempts = 4;
        final var redeliveryQueue = "test";

        final var config = new ImmutableKDLQConfiguration.Redelivery(
                redeliveryQueue,
                maxRedeliveryAttempts,
                multiplier,
                redeliveryDelay,
                maxRedeliveryDelay
        );

        assertEquals(maxRedeliveryAttempts, config.maxRedeliveryAttemptsBeforeKill(), "Redelivery attempts must be equal");
        assertEquals(redeliveryDelay, config.redeliveryDelay(), "Redelivery delay must be equal");
        assertEquals(multiplier, config.redeliveryDelayMultiplier(), "Redelivery delay multiplier must be equal");
        assertEquals(maxRedeliveryDelay, config.maxRedeliveryDelay(), "Max redelivery delay must be equal");
        assertEquals(redeliveryQueue, config.redeliveryQueueName(), "Redelivery queue must be equal");
    }

    @Test
    void testRedeliveryCreationWithInvalidRedeliveryDelayMultiplier() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> new ImmutableKDLQConfiguration.Redelivery(
                        "redelivery-topic",
                        10,
                        0,
                        1000,
                        5000
                )
        );
    }

    @Test
    void testRedeliveryCreationWithNegativeRedeliveryDelay() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> new ImmutableKDLQConfiguration.Redelivery(
                        "redelivery-topic",
                        10,
                        2.0,
                        -1,
                        5000
                )
        );
    }

    @Test
    void testRedeliveryCreationWithNegativeMaxRedeliveryDelay() {
        assertThrows(
                KDLQConfigurationException.class,
                () -> new ImmutableKDLQConfiguration.Redelivery(
                        "redelivery-topic",
                        10,
                        2.0,
                        1000,
                        -1
                )
        );
    }

    @Test
    void testDLQBuilderSetMaxKills() {
        ImmutableKDLQConfiguration.DLQ.Builder builder = new ImmutableKDLQConfiguration.DLQ.Builder();
        ImmutableKDLQConfiguration.DLQ.Builder updatedBuilder = builder.withMaxKills(10);
        assertEquals(updatedBuilder, builder); //Ensure the builder method returns the same instance
        assertEquals(10, updatedBuilder.build("dlq-topic").maxKills());
    }

    @Test
    void testRedeliveryConfigCreation() {
        final int maxRedeliveryDelay = 5000;
        final int redeliveryDelay = 50;
        final double multiplier = 2.0;
        final int maxRedeliveryAttempts = 4;
        final var redeliveryQueue = "test";

        final var config =
                ImmutableKDLQConfiguration.Redelivery.builder()
                    .withRedeliveryDelay(redeliveryDelay)
                    .withMaxRedeliveryDelay(maxRedeliveryDelay)
                    .withMaxRedeliveryAttemptsBeforeKill(maxRedeliveryAttempts)
                    .withRedeliveryDelayMultiplier(multiplier)
                    .withRedeliveryQueueName(redeliveryQueue)
                .build();

        assertEquals(maxRedeliveryAttempts, config.maxRedeliveryAttemptsBeforeKill(), "Redelivery attempts must be equal");
        assertEquals(redeliveryDelay, config.redeliveryDelay(), "Redelivery delay must be equal");
        assertEquals(multiplier, config.redeliveryDelayMultiplier(), "Redelivery delay multiplier must be equal");
        assertEquals(maxRedeliveryDelay, config.maxRedeliveryDelay(), "Max redelivery delay must be equal");
        assertEquals(redeliveryQueue, config.redeliveryQueueName(), "Redelivery queue must be equal");
    }

    private void makeConfigurationChecks(
            final KDLQConfiguration config,
            final Set<String> expectedServers,
            final String expectedDLQ,
            final String expectedRedeliveryQueue,
            final int maxKills,
            final int maxRedeliveryAttemptsBeforeKill,
            final Map<String, Object> producerProperties
    ) {
        assertNotNull(config, "Created config must be not null");
        assertEquals(expectedServers, config.bootstrapServers(), "Servers must be equal");
        assertEquals(expectedDLQ, config.dlq().deadLetterQueueName(), "DLQ must be equal");
        assertEquals(expectedRedeliveryQueue, config.redelivery().redeliveryQueueName(), "Redelivery queue must be equal");
        assertEquals(maxKills, config.dlq().maxKills(), "Max kills count must be equal");
        assertEquals(maxRedeliveryAttemptsBeforeKill, config.redelivery().maxRedeliveryAttemptsBeforeKill(), "Max redelivery attempts count must be equal");
        assertEquals(producerProperties, config.producerProperties(), "Producer properties must be equal");
        assertNotNull(config.id(), "Id must be not null");
        assertFalse(config.id().isBlank(), "Id must be not blank");
    }

    private static class TestListener implements KDLQMessageLifecycleListener {

    }
}
