package ru.joke.kdlq.internal.routers.headers;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.*;

class KDLQHeadersServiceTest {

    private static final String INT_HEADER = "h1";
    private static final String STRING_HEADER = "h2";
    private static final String LONG_HEADER = "h3";

    private final KDLQHeadersService headersService = new KDLQHeadersService();

    @Test
    void testEmptyIntHeader() {
        final var nullHeaderValue = this.headersService.getIntHeader(null);
        assertNotNull(nullHeaderValue, "Header value wrapper must be not null");
        assertTrue(nullHeaderValue.isEmpty(), "Header value wrapper must be empty");

        final var emptyBytesHeaderValue = this.headersService.getIntHeader(new RecordHeader("1", new byte[0]));
        assertNotNull(emptyBytesHeaderValue, "Header value wrapper must be not null");
        assertTrue(emptyBytesHeaderValue.isEmpty(), "Header value wrapper must be empty");
    }

    @Test
    void testEmptyStringHeader() {
        final int intHeaderValue = 5;
        final var intHeader = this.headersService.createIntHeader(INT_HEADER, intHeaderValue);
        final var nullHeaderValue = this.headersService.getStringHeader(STRING_HEADER, new RecordHeaders(List.of(intHeader)));
        assertNotNull(nullHeaderValue, "Header value wrapper must be not null");
        assertTrue(nullHeaderValue.isEmpty(), "Header value wrapper must be empty");
    }

    @Test
    void testIntHeader() {
        final int headerValue = 5;
        final var header = this.headersService.createIntHeader(INT_HEADER, headerValue);

        makeHeaderChecks(header, INT_HEADER);

        final var valueFromHeader = this.headersService.getIntHeader(header);
        makeIntHeaderChecks(valueFromHeader, headerValue);
    }

    @Test
    void testLongHeader() {
        final long headerValue = 5;
        final var header = this.headersService.createLongHeader(LONG_HEADER, headerValue);

        makeHeaderChecks(header, LONG_HEADER);
    }

    @Test
    void testIntHeaderFromHeaders() {
        final int headerValue = 2;
        final var intHeader = this.headersService.createIntHeader(INT_HEADER, headerValue);
        final var stringHeader = this.headersService.createStringHeader(STRING_HEADER, "v1");

        final var headers = new RecordHeaders(List.of(intHeader, stringHeader));

        final var valueFromHeader = this.headersService.getIntHeader(INT_HEADER, headers);
        makeIntHeaderChecks(valueFromHeader, headerValue);
    }

    @Test
    void testStringHeaders() {
        final var headerValue1 = "v1";
        final var headerValue2 = "v2";
        final var headerName1 = "h1";
        final var headerName2 = "h2";

        final var stringHeader1 = this.headersService.createStringHeader(headerName1, headerValue1);
        final var stringHeader2 = this.headersService.createStringHeader(headerName2, headerValue2);

        final var headers = new RecordHeaders(List.of(stringHeader1, stringHeader2));

        makeHeaderChecks(stringHeader1, headerName1);
        makeHeaderChecks(stringHeader2, headerName2);

        final var valueFromHeader1 = this.headersService.getStringHeader(headerName1, headers);
        assertTrue(valueFromHeader1.isPresent(), "String header value must present");
        assertEquals(headerValue1, valueFromHeader1.get(), "String header value must be equal");
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
