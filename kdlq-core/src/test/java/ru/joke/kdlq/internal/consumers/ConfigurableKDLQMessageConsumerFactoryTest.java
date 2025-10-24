package ru.joke.kdlq.internal.consumers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.ImmutableKDLQConfiguration;
import ru.joke.kdlq.KDLQException;
import ru.joke.kdlq.KDLQMessageConsumer;
import ru.joke.kdlq.KDLQMessageProcessor;
import ru.joke.kdlq.internal.configs.InternalKDLQConfigurationRegistry;
import ru.joke.kdlq.internal.configs.KDLQConfigurationRegistry;
import ru.joke.kdlq.internal.routers.InternalKDLQMessageRouterFactory;
import ru.joke.kdlq.internal.routers.KDLQMessageRouter;
import ru.joke.kdlq.internal.routers.KDLQMessageRouterFactory;
import ru.joke.kdlq.internal.routers.RoutersUtil;
import ru.joke.kdlq.internal.routers.producers.KDLQProducersRegistry;

import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.*;

class ConfigurableKDLQMessageConsumerFactoryTest {

    private KDLQConfigurationRegistry registry;
    private KDLQMessageRouterFactory routerFactory;
    private Consumer<KDLQMessageConsumer<?, ?>> callback;

    @BeforeEach
    void setUp() {
        this.registry = mock(InternalKDLQConfigurationRegistry.class);
        this.routerFactory = mock(InternalKDLQMessageRouterFactory.class);
        this.callback = mock(Consumer.class);
    }

    @Test
    void testFactoryConstructorValidation() {
        assertThrows(
                KDLQException.class,
                () -> new ConfigurableKDLQMessageConsumerFactory(
                        this.registry,
                        this.routerFactory,
                        null
                )
        );

        assertThrows(
                KDLQException.class,
                () -> new ConfigurableKDLQMessageConsumerFactory(
                        this.registry,
                        null,
                        this.callback
                )
        );

        assertThrows(
                KDLQException.class,
                () -> new ConfigurableKDLQMessageConsumerFactory(
                        null,
                        this.routerFactory,
                        this.callback
                )
        );
    }

    @Test
    void testConsumerCreationWithProvidedConfiguration() {
        final var factory = new ConfigurableKDLQMessageConsumerFactory(
                this.registry,
                this.routerFactory,
                this.callback
        );

        final var processor = mock(KDLQMessageProcessor.class);
        final var config = mock(ImmutableKDLQConfiguration.class);
        final var id = "1";

        when(this.routerFactory.create(eq(id), same(config))).thenReturn(RoutersUtil.createRouterMock());

        final var consumer = factory.create(id, config, processor);

        assertNotNull(consumer, "Consumer must be not null");
        assertEquals(id, consumer.id(), "Consumer id must be equal");
        verify(this.routerFactory).create(eq(id), same(config));
        verifyNoInteractions(this.registry);
    }

    @Test
    void testConsumerCreationWithProvidedConfigurationIdWhenConfigIsRegistered() {
        final var factory = new ConfigurableKDLQMessageConsumerFactory(
                this.registry,
                this.routerFactory,
                this.callback
        );

        final var processor = mock(KDLQMessageProcessor.class);
        final var config = mock(ImmutableKDLQConfiguration.class);
        final var id = "1";
        final var configId = "cfg";

        when(this.registry.get(configId)).thenReturn(Optional.of(config));
        when(this.routerFactory.create(eq(id), same(config))).thenReturn(RoutersUtil.createRouterMock());

        final var consumer = factory.create(id, configId, processor);

        assertNotNull(consumer, "Consumer must be not null");
        assertEquals(id, consumer.id(), "Consumer id must be equal");
        verify(this.routerFactory).create(eq(id), same(config));
        verify(this.registry).get(eq(configId));
    }

    @Test
    void testConsumerCreationWithProvidedConfigurationIdWhenConfigIsNotRegistered() {
        final var factory = new ConfigurableKDLQMessageConsumerFactory(
                this.registry,
                this.routerFactory,
                this.callback
        );

        final var processor = mock(KDLQMessageProcessor.class);
        final var id = "1";
        final var configId = "cfg";

        when(this.registry.get(configId)).thenReturn(Optional.empty());

        assertThrows(KDLQException.class, () -> factory.create(id, configId, processor));
        verifyNoInteractions(this.routerFactory);
        verify(this.registry).get(eq(configId));
    }
}
