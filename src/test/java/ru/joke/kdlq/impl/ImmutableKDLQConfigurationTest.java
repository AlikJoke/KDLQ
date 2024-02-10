package ru.joke.kdlq.impl;

import org.junit.jupiter.api.Test;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQConfigurationException;
import ru.joke.kdlq.KDLQMessageLifecycleListener;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class ImmutableKDLQConfigurationTest {

    private static final String TEST_QUEUE_1 = "test1";
    private static final String TEST_QUEUE_2 = "test2";

    @Test
    public void testWhenIncorrectConfigurationThenExceptionShouldBeThrown() {
        assertThrows(KDLQConfigurationException.class, () -> ImmutableKDLQConfiguration.builder().build(Collections.emptySet(), TEST_QUEUE_1));
        assertThrows(KDLQConfigurationException.class, () -> ImmutableKDLQConfiguration.builder().build(Set.of("1"), ""));
        assertThrows(KDLQConfigurationException.class, () -> ImmutableKDLQConfiguration.builder().build(Set.of("1"), " "));
    }

    @Test
    public void testWhenCorrectConfigurationProvidedWithoutOptionalFields() {
        final var servers = Set.of("1", "2");
        final var config = ImmutableKDLQConfiguration.builder().build(servers, TEST_QUEUE_2);

        makeConfigurationChecks(config, servers, TEST_QUEUE_2, null, -1, -1, Collections.emptyMap(), Collections.emptySet());
    }

    @Test
    public void testWhenFullCorrectConfigurationProvided() {
        final var servers = Set.of("1", "2");
        final var maxKills = 5;
        final var maxRedeliveryAttemptsBeforeKill = 3;
        final Map<String, Object> producerProperties = Map.of("p1", "1", "p2", 2, "p3", true);
        final Set<KDLQMessageLifecycleListener> listeners = Set.of(new TestListener(), new TestListener());
        final var config =
                ImmutableKDLQConfiguration
                        .builder()
                            .withMaxKills(maxKills)
                            .withMaxRedeliveryAttemptsBeforeKill(maxRedeliveryAttemptsBeforeKill)
                            .withProducerProperties(producerProperties)
                            .withLifecycleListeners(listeners)
                            .withRedeliveryQueueName(TEST_QUEUE_2)
                        .build(servers, TEST_QUEUE_1);

        makeConfigurationChecks(config, servers, TEST_QUEUE_1, TEST_QUEUE_2, maxKills, maxRedeliveryAttemptsBeforeKill, producerProperties, listeners);
    }

    @Test
    public void testConfigurationIdEqualityContract() {
        final var servers = Set.of("1", "2");
        final var config1 = ImmutableKDLQConfiguration.builder().build(servers, TEST_QUEUE_2);
        final Map<String, Object> producerProperties = Map.of("p1", "1", "p2", 2, "p3", true);

        final var config2 =
                ImmutableKDLQConfiguration
                        .builder()
                            .withMaxKills(2)
                            .withMaxRedeliveryAttemptsBeforeKill(3)
                            .withLifecycleListener(new TestListener())
                            .withRedeliveryQueueName(TEST_QUEUE_2)
                        .build(servers, TEST_QUEUE_1);

        final var config3 =
                ImmutableKDLQConfiguration
                        .builder()
                            .withMaxKills(2)
                            .withMaxRedeliveryAttemptsBeforeKill(3)
                            .withLifecycleListener(new TestListener())
                            .withProducerProperties(producerProperties)
                            .withRedeliveryQueueName(TEST_QUEUE_2)
                        .build(servers, TEST_QUEUE_1);
        final var config4 =
                ImmutableKDLQConfiguration
                    .builder()
                        .withProducerProperties(producerProperties)
                    .build(servers, TEST_QUEUE_2);

        assertEquals(config1.id(), config2.id(), "Ids must be equal");
        assertNotEquals(config1.id(), config3.id(), "Ids must be not equal");
        assertEquals(config3.id(), config4.id(), "Ids must be equal");
    }

    private void makeConfigurationChecks(
            final KDLQConfiguration config,
            final Set<String> expectedServers,
            final String expectedDLQ,
            final String expectedRedeliveryQueue,
            final int maxKills,
            final int maxRedeliveryAttemptsBeforeKill,
            final Map<String, Object> producerProperties,
            final Set<KDLQMessageLifecycleListener> listeners) {
        assertNotNull(config, "Created config must be not null");
        assertEquals(expectedServers, config.bootstrapServers(), "Servers must be equal");
        assertEquals(expectedDLQ, config.deadLetterQueueName(), "DLQ must be equal");
        assertEquals(expectedRedeliveryQueue, config.redeliveryQueueName(), "Redelivery queue must be equal");
        assertEquals(maxKills, config.maxKills(), "Max kills count must be equal");
        assertEquals(maxRedeliveryAttemptsBeforeKill, config.maxRedeliveryAttemptsBeforeKill(), "Max redelivery attempts count must be equal");
        assertEquals(producerProperties, config.producerProperties(), "Producer properties must be equal");
        assertEquals(listeners, config.lifecycleListeners(), "Listeners must be equal");
        assertNotNull(config.id(), "Id must be not null");
        assertFalse(config.id().isBlank(), "Id must be not blank");
    }

    private static class TestListener implements KDLQMessageLifecycleListener {

    }
}
