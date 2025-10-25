package ru.joke.kdlq.internal.configs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.ImmutableKDLQConfiguration;
import ru.joke.kdlq.KDLQConfiguration;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class InternalKDLQConfigurationRegistryTest {

    private KDLQConfigurationRegistry registry;
    private KDLQConfiguration config1;
    private KDLQConfiguration config2;
    private String id1;
    private String id2;

    @BeforeEach
    void setUp() {
        this.registry = new InternalKDLQConfigurationRegistry();
        this.config1 = mock(ImmutableKDLQConfiguration.class);
        this.config2 = mock(ImmutableKDLQConfiguration.class);
        this.id1 = UUID.randomUUID().toString();
        this.id2 = UUID.randomUUID().toString();

        when(config1.id()).thenReturn(id1);
        when(config2.id()).thenReturn(id2);
    }

    @Test
    void testRegisterSuccess() {
        assertTrue(registry.register(config1), "Configuration must be registered");

        final var actual = registry.get(id1);
        assertNotNull(actual, "Result must be not null");
        assertTrue(actual.isPresent(), "Result must be not empty");
        assertSame(config1, actual.get(), "Configuration must be same");
    }

    @Test
    void testDuplicateRegistration() {
        assertTrue(registry.register(config1), "Configuration must be registered");
        assertFalse(registry.register(config1), "Configuration must not be registered");
    }

    @Test
    void testUnregister() {
        registry.register(config1);

        assertTrue(registry.unregister(config1), "Configuration must be unregistered");

        final var result = registry.get(id1);
        assertNotNull(result, "Result must be not null");
        assertFalse(result.isPresent(), "Result must be empty");

        assertFalse(registry.unregister(id1), "Configuration must not present in registry");
    }

    @Test
    void testWhenUnregisterNotExistentConfigThenReturnsFalse() {
        assertFalse(registry.unregister("non-existent-id"), "Configuration must not present in registry");
    }

    @Test
    void testWhenGetAllOnEmptyRegistryThenReturnsEmptySet() {
        final var all = registry.getAll();
        assertTrue(all.isEmpty(), "Configurations set must be empty");
    }

    @Test
    void testWhenGetAllReturnsAllConfigurations() {
        registry.register(config1);
        registry.register(config2);

        final var all = registry.getAll();
        assertEquals(2, all.size(), "Count of configs must be equal");
        assertTrue(all.contains(config1), "Registry must contain config");
        assertTrue(all.contains(config2), "Registry must contain config");
    }

    @Test
    void testWhenGetAllReturnsNewSet() {
        registry.register(config1);
        final var all1 = registry.getAll();
        registry.unregister(id1);

        final var all2 = registry.getAll();
        assertEquals(1, all1.size(), "Configs count must be equal");
        assertEquals(0, all2.size(), "Configs count must be equal");
    }
}
