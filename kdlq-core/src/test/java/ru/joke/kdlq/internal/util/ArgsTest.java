package ru.joke.kdlq.internal.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.KDLQException;

import java.util.*;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class ArgsTest {

    private KDLQException expectedException;
    private Supplier<KDLQException> exceptionSupplier;

    @BeforeEach
    void setUp() {
        this.expectedException = new KDLQException("Constraint violation");
        this.exceptionSupplier = () -> this.expectedException;
    }

    @Test
    void testRequireNotNullWhenValidValueThenReturnsValue() {
        final var expected = new Object();
        final var actual = Args.requireNotNull(expected, exceptionSupplier);
        assertSame(expected, actual, "Value must be same");
    }

    @Test
    void testRequireNotNullWhenNullValueThenThrowsException() {
        final var thrown = assertThrows(
                KDLQException.class,
                () -> Args.requireNotNull(null, exceptionSupplier)
        );

        assertSame(expectedException, thrown, "Exception must be same");
    }

    @Test
    void testRequireNotEmptyStringWhenNotEmptyStringThenReturnsValue() {
        final var expected = "test";
        final var actual = Args.requireNotEmpty(expected, exceptionSupplier);
        assertSame(expected, actual, "String must be same");
    }

    @Test
    void testRequireNotEmptyStringWhenInvalidStringThenThrowsException() {
        for (var invalidString : new String[] { null, "" }) {
            final var thrown = assertThrows(
                    KDLQException.class,
                    () -> Args.requireNotEmpty(invalidString, exceptionSupplier)
            );

            assertSame(expectedException, thrown, "Exception must be same");
        }
    }

    @Test
    void testRequirePositiveWhenValidValueThenReturnsValue() {
        int expected = 1;
        final var actual = Args.requirePositive(expected, exceptionSupplier);
        assertEquals(expected, actual, "Value must be equal");
    }

    @Test
    void testRequirePositiveWhenInvalidValueThenThrowsException() {
        for (int invalidValue : new int[] { -1, 0 }) {
            final var thrown = assertThrows(
                    KDLQException.class,
                    () -> Args.requirePositive(invalidValue, exceptionSupplier)
            );

            assertSame(expectedException, thrown, "Exception must be same");
        }
    }

    @Test
    void testRequireNonNegativeIntWhenValidIntThenReturnsValue() {
        for (int expected : new int[] {0, 1}) {
            final var actual = Args.requireNonNegative(expected, exceptionSupplier);
            assertEquals(expected, actual, "Value must be equal");
        }
    }

    @Test
    void testRequireNonNegativeIntWhenNegativeIntThenThrowsException() {
        final var thrown = assertThrows(
                KDLQException.class,
                () -> Args.requireNonNegative((int) -1, exceptionSupplier)
        );

        assertSame(expectedException, thrown, "Exception must be same");
    }

    @Test
    void testRequireNonNegativeLongWhenValidLongThenReturnsValue() {
        for (final var expected : new long[] { 0L, 2L }) {
            final var actual = Args.requireNonNegative(expected, exceptionSupplier);
            assertEquals(expected, actual, "Value must be equal");
        }
    }

    @Test
    void testRequireNonNegativeLongWhenNegativeLongThenThrowsException() {
        final var thrown = assertThrows(
                KDLQException.class,
                () -> Args.requireNonNegative(-1L, exceptionSupplier)
        );

        assertSame(expectedException, thrown, "Exception must be same");
    }

    @Test
    void testRequireNotEmptyCollectionWhenNotEmptyCollectionThenReturnsValue() {
        final var expectedColl = List.of("v1", "v2");
        final var actualColl = Args.requireNotEmpty(expectedColl, exceptionSupplier);
        assertSame(expectedColl, actualColl, "Collection must be same");
    }

    @Test
    void testRequireNotEmptyCollectionWhenInvalidCollectionThenThrowsException() {
        for (var invalidColl : new Collection[] { null, Collections.emptySet() }) {
            final var thrown = assertThrows(
                    KDLQException.class,
                    () -> Args.requireNotEmpty(invalidColl, exceptionSupplier)
            );

            assertSame(expectedException, thrown, "Exception must be same");
        }
    }

    @Test
    void testRequireNotEmptyMapWhenNotEmptyMapThenReturnsValue() {
        final var expectedMap = Map.of("v1", "v2");
        final var actualMap = Args.requireNotEmpty(expectedMap, exceptionSupplier);
        assertSame(expectedMap, actualMap, "Collection must be same");
    }

    @Test
    void testRequireNotEmptyMapWhenInvalidMapThenThrowsException() {
        for (var invalidMap : new Map[] { null, Collections.emptyMap() }) {
            final var thrown = assertThrows(
                    KDLQException.class,
                    () -> Args.requireNotEmpty(invalidMap, exceptionSupplier)
            );

            assertSame(expectedException, thrown, "Exception must be same");
        }
    }
}
