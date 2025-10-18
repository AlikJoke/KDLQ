package ru.joke.kdlq.impl.internal;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.impl.internal.routers.KDLQHeadersService;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.*;

public class KDLQHeadersServiceTest {

    private static final String INT_HEADER = "h1";
    private static final String STRING_HEADER = "h2";

    private final KDLQHeadersService headersService = new KDLQHeadersService();

    @Test
    public void testEmptyIntHeader() {
        final var nullHeaderValue = this.headersService.getIntHeader(null);
        assertNotNull(nullHeaderValue, "Header value wrapper must be not null");
        assertTrue(nullHeaderValue.isEmpty(), "Header value wrapper must be empty");

        final var emptyBytesHeaderValue = this.headersService.getIntHeader(new RecordHeader("1", new byte[0]));
        assertNotNull(emptyBytesHeaderValue, "Header value wrapper must be not null");
        assertTrue(emptyBytesHeaderValue.isEmpty(), "Header value wrapper must be empty");
    }

    @Test
    public void testIntHeader() {
        final int headerValue = 5;
        final var header = this.headersService.createIntHeader(INT_HEADER, headerValue);

        makeHeaderChecks(header, INT_HEADER);

        final var valueFromHeader = this.headersService.getIntHeader(header);
        makeIntHeaderChecks(valueFromHeader, headerValue);
    }

    @Test
    public void testIntHeaderFromHeaders() {
        final int headerValue = 2;
        final var intHeader = this.headersService.createIntHeader(INT_HEADER, headerValue);
        final var stringHeader = this.headersService.createStringHeader(STRING_HEADER, "v1");

        final var headers = new RecordHeaders(List.of(intHeader, stringHeader));

        final var valueFromHeader = this.headersService.getIntHeader(INT_HEADER, headers);
        makeIntHeaderChecks(valueFromHeader, headerValue);
    }

    @Test
    public void testStringHeader() {
        final var headerValue = "v1";
        final var header = this.headersService.createStringHeader(STRING_HEADER, headerValue);

        makeHeaderChecks(header, STRING_HEADER);

        final var valueFromHeader = new String(header.value(), StandardCharsets.UTF_8);
        assertNotNull(valueFromHeader, "Header value must be not null");
        assertEquals(headerValue, valueFromHeader, "Header value must be equal");
    }

    private void makeIntHeaderChecks(final OptionalInt valueFromHeader, final int expectedHeaderValue) {
        assertNotNull(valueFromHeader, "Header value must be not null");
        assertFalse(valueFromHeader.isEmpty(), "Header value must be not empty");
        assertEquals(expectedHeaderValue, valueFromHeader.getAsInt(), "Header value must be equal");
    }

    private void makeHeaderChecks(final Header header, final String expectedHeaderKey) {
        assertEquals(expectedHeaderKey, header.key(), "Header name must be equal");
        assertNotNull(header.value(), "Header value must be not null");
    }
}
