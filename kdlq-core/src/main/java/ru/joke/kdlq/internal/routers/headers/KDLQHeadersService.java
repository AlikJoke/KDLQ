package ru.joke.kdlq.internal.routers.headers;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Service for handling KDLQ-added headers.
 *
 * @author Alik
 * @see KDLQHeaders
 */
public final class KDLQHeadersService {

    /**
     * Creates header for int value with provided name.
     *
     * @param headerName provided header name; cannot be {@code null} or empty.
     * @param value      provided int value.
     * @return created header; cannot be {@code null}.
     */
    @Nonnull
    public Header createIntHeader(@Nonnull final String headerName, final int value) {
        return new RecordHeader(headerName, intToByteArray(value));
    }

    /**
     * Creates header for long value with provided name.
     *
     * @param headerName provided header name; cannot be {@code null} or empty.
     * @param value      provided long value.
     * @return created header; cannot be {@code null}.
     */
    @Nonnull
    public Header createLongHeader(@Nonnull final String headerName, final long value) {
        return new RecordHeader(headerName, longToByteArray(value));
    }

    /**
     * Creates header for string value with provided name.
     *
     * @param headerName provided header name; cannot be {@code null} or empty.
     * @param value      provided string value; cannot be {@code null}.
     * @return created header; cannot be {@code null}.
     */
    @Nonnull
    public Header createStringHeader(@Nonnull final String headerName, final String value) {
        return new RecordHeader(headerName, value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Extracts an integer value from the header with the specified name within the provided headers.
     *
     * @param headerName provided header name; cannot be {@code null} or empty.
     * @param headers    provided headers; cannot be {@code null}.
     * @return integer value wrapped in {@link OptionalInt}; cannot be {@code null}.
     */
    @Nonnull
    public OptionalInt getIntHeader(@Nonnull final String headerName, @Nonnull final Headers headers) {
        final var header = headers.lastHeader(headerName);
        return getIntHeader(header);
    }

    /**
     * Extracts a string value from the header with the specified name within the provided headers.
     *
     * @param headerName provided header name; cannot be {@code null} or empty.
     * @param headers    provided headers; cannot be {@code null}.
     * @return string value wrapped in {@link Optional}; cannot be {@code null}.
     */
    @Nonnull
    public Optional<String> getStringHeader(@Nonnull final String headerName, @Nonnull final Headers headers) {
        final var header = headers.lastHeader(headerName);
        if (header == null) {
            return Optional.empty();
        }

        final var headerBytes = header.value();
        final var headerValue = new String(headerBytes, StandardCharsets.UTF_8);
        return Optional.of(headerValue);
    }

    /**
     * Extracts an integer value from the provided header.
     *
     * @param header provided header; cannot be {@code null}.
     * @return integer value wrapped in {@link OptionalInt}; cannot be {@code null}.
     */
    @Nonnull
    public OptionalInt getIntHeader(final Header header) {
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
        final int length = result.length;
        for (int i = 0; i < length; i++) {
            result[length - i - 1] = (byte) ((value >> 8 * i) & 0xFF);
        }

        return result;
    }

    private byte[] longToByteArray(final long value) {
        final byte[] result = new byte[Long.BYTES];
        final int length = result.length;
        for (int i = 0; i < length; i++) {
            result[length - i - 1] = (byte) ((value >> 8 * i) & 0xFF);
        }

        return result;
    }
}
