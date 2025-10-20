package ru.joke.kdlq.internal.util;

import java.util.Collection;
import java.util.function.Supplier;

public abstract class Args {

    public static <T> T requireNotNull(
            final T value,
            final Supplier<RuntimeException> exceptionSupplier
    ) {
        if (value == null) {
            throw exceptionSupplier.get();
        }

        return value;
    }

    public static String requireNotEmpty(
            final String value,
            final Supplier<RuntimeException> exceptionSupplier
    ) {
        if (value == null || value.isEmpty()) {
            throw exceptionSupplier.get();
        }

        return value;
    }

    public static int requirePositive(
            final int value,
            final Supplier<RuntimeException> exceptionSupplier
    ) {
        if (value <= 0) {
            throw exceptionSupplier.get();
        }

        return value;
    }

    public static int requireNonNegative(
            final int value,
            final Supplier<RuntimeException> exceptionSupplier
    ) {
        if (value < 0) {
            throw exceptionSupplier.get();
        }

        return value;
    }

    public static <T> Collection<T> requireNotEmpty(
            final Collection<T> value,
            final Supplier<RuntimeException> exceptionSupplier
    ) {
        if (value == null || value.isEmpty()) {
            throw exceptionSupplier.get();
        }

        return value;
    }
}
