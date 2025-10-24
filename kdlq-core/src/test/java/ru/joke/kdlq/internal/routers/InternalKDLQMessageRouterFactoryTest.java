package ru.joke.kdlq.internal.routers;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.ImmutableKDLQConfiguration;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQException;
import ru.joke.kdlq.internal.routers.headers.KDLQHeadersService;
import ru.joke.kdlq.internal.routers.producers.InternalKDLQMessageProducerFactory;
import ru.joke.kdlq.internal.routers.producers.InternalKDLQProducersRegistry;
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducerFactory;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class InternalKDLQMessageRouterFactoryTest {

    @Test
    void testFactoryConstructionValidation() {
        final var producerFactory = mock(InternalKDLQMessageProducerFactory.class);
        final var headersService = mock(KDLQHeadersService.class);
        assertThrows(
                KDLQException.class,
                () -> new InternalKDLQMessageRouterFactory(
                        null,
                        () -> null,
                        mock(KDLQHeadersService.class)
                )
        );

        assertThrows(
                KDLQException.class,
                () -> new InternalKDLQMessageRouterFactory(
                        producerFactory,
                        null,
                        mock(KDLQHeadersService.class)
                )
        );

        assertThrows(
                KDLQException.class,
                () -> new InternalKDLQMessageRouterFactory(
                        producerFactory,
                        () -> null,
                        null
                )
        );
    }

    @Test
    void testRouterCreation() {
        final var producerFactory = mock(InternalKDLQMessageProducerFactory.class);
        final var factory = new InternalKDLQMessageRouterFactory(
                producerFactory,
                () -> null,
                mock(KDLQHeadersService.class)
        );

        final Map<String, Object> producerProperties = Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
        );

        final var config = mock(ImmutableKDLQConfiguration.class);
        when(config.producerProperties()).thenReturn(producerProperties);

        final var router = factory.create("test", config);

        assertNotNull(router, "Router must be not null");
        verify(producerFactory).create(eq(config));
    }
}
