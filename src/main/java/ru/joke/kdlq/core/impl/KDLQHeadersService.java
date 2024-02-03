package ru.joke.kdlq.core.impl;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import javax.annotation.Nonnull;
import java.util.OptionalInt;

final class KDLQHeadersService {

    @Nonnull
    Header createIntHeader(@Nonnull final String headerName, final int value) {
        return new RecordHeader(headerName, intToByteArray(value));
    }

    @Nonnull
    OptionalInt getIntHeader(@Nonnull final String headerName, @Nonnull final Headers headers) {
        final var header = headers.lastHeader(headerName);
        return getIntHeader(header);
    }

    @Nonnull
    OptionalInt getIntHeader(final Header header) {
        return header == null || header.value().length == 0
                ? OptionalInt.empty()
                : OptionalInt.of(byteArrayToInt(header.value()));
    }

    private int byteArrayToInt(final byte[] array) {
        int value = 0;
        for (final byte b : array) {
            value = (value << 8) + (b & 0xFF);
        }

        return value;
    }

    private byte[] intToByteArray(final int value) {
        final byte[] result = new byte[Integer.BYTES];
        int length = result.length;
        for (int i = 0; i < length; i++) {
            result[length - i - 1] = (byte) ((value >> 8 * i) & 0xFF);
        }

        return result;
    }
}
