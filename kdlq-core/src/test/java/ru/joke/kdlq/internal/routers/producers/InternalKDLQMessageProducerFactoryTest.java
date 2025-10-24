package ru.joke.kdlq.internal.routers.producers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQException;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class InternalKDLQMessageProducerFactoryTest {

    private KDLQMessageProducerFactory factory;

    @BeforeEach
    void setUp() {
        final var registry = mock(InternalKDLQProducersRegistry.class);
        this.factory = new InternalKDLQMessageProducerFactory(registry);
    }

    @Test
    void testFactoryConstructorValidation() {
        assertThrows(
                KDLQException.class,
                () -> new InternalKDLQMessageProducerFactory(null)
        );
    }

    @Test
    void testWhenConfigurationProvided() {
        final var dlqConfig = KDLQConfiguration.DLQ.builder().build("test");
        final var config = KDLQConfiguration.builder().build(
                Set.of("test"),
                Map.of("p1", "v1"),
                dlqConfig
        );
        final var producer = this.factory.create(config);

        assertNotNull(producer, "Producer must be not null");
    }

    @Test
    void testWhenNoConfigurationProvided() {
        assertThrows(KDLQException.class, () -> this.factory.create(null));
    }
}
