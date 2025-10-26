package ru.joke.kdlq.internal.redelivery;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.joke.kdlq.*;
import ru.joke.kdlq.internal.configs.InternalKDLQConfigurationRegistry;
import ru.joke.kdlq.internal.configs.KDLQConfigurationRegistry;
import ru.joke.kdlq.internal.routers.headers.KDLQHeadersService;
import ru.joke.kdlq.internal.routers.producers.InternalKDLQMessageProducerFactory;
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducer;
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducerFactory;
import ru.joke.kdlq.spi.KDLQGlobalDistributedLockService;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static ru.joke.kdlq.internal.routers.headers.KDLQHeaders.MESSAGE_PARTITION_HEADER;

class RedeliveryTaskMessagesProcessingTest {

    private ScheduledExecutorService redeliveryDispatcherPool;
    private ScheduledFuture<?> dispatcherFuture;
    private KDLQGlobalConfiguration globalConfiguration;
    private KDLQConfigurationRegistry configurationRegistry;
    private KDLQMessageProducerFactory producerFactory;
    private KDLQMessageProducer<byte[], byte[]> producer;
    private RedeliveryTask task;
    private KDLQHeadersService headersService;
    private KDLQConfiguration configuration;

    @BeforeEach
    void setUp() {
        this.redeliveryDispatcherPool = mock(ScheduledExecutorService.class);
        this.dispatcherFuture = mock(ScheduledFuture.class);
        this.configuration = prepareMockConfig();
        this.producer = mock(KDLQMessageProducer.class);

        this.producerFactory = mock(InternalKDLQMessageProducerFactory.class);
        when(this.producerFactory.create(any())).then(i -> producer);

        this.configurationRegistry = mock(InternalKDLQConfigurationRegistry.class);

        this.globalConfiguration = KDLQGlobalConfigurationFactory.getInstance().createStatelessStandaloneConfiguration(
                this.redeliveryDispatcherPool,
                null,
                10L
        );
        this.headersService = new KDLQHeadersService();
        this.task = new RedeliveryTask(
                this.globalConfiguration,
                this.configurationRegistry,
                this.producerFactory,
                this.headersService
        );
    }

    @Test
    void testMessagesRedelivery() throws Exception {
        when(redeliveryDispatcherPool.schedule(
                same(this.task),
                eq(this.globalConfiguration.redeliveryDispatcherTaskDelay()),
                eq(TimeUnit.MILLISECONDS))
        ).then(i -> dispatcherFuture);

        final var record1 = createMockRecord(10L);
        final var record2 = createMockRecord(20L);
        final var record3 = createMockRecord(System.currentTimeMillis() + 100_000);

        this.globalConfiguration.redeliveryStorage().store(record1);
        this.globalConfiguration.redeliveryStorage().store(record2);
        this.globalConfiguration.redeliveryStorage().store(record3);

        this.task.run();
        this.task.run();

        this.globalConfiguration.redeliveryPool().shutdown();

        verify(redeliveryDispatcherPool, times(2)).schedule(
                same(this.task),
                eq(this.globalConfiguration.redeliveryDispatcherTaskDelay()),
                eq(TimeUnit.MILLISECONDS)
        );

        final var readyToRedelivery = this.globalConfiguration.redeliveryStorage().findAllReadyToRedelivery(c -> null, Long.MAX_VALUE, Integer.MAX_VALUE);
        assertEquals(1, readyToRedelivery.size(), "Count of leave messages in storage must be equal");
        assertEquals(record3.nextRedeliveryTimestamp(), readyToRedelivery.getFirst().nextRedeliveryTimestamp(), "Leave message must be correct");

        verify(this.producer).send(same(record1.record()));
        verify(this.producer).send(same(record2.record()));
        verify(this.producer).close();
    }

    private KDLQConfiguration prepareMockConfig() {
        final var configuration = mock(ImmutableKDLQConfiguration.class);
        when(configuration.bootstrapServers()).thenReturn(Set.of("test"));
        when(configuration.lifecycleListeners()).thenReturn(Collections.emptySet());
        when(configuration.dlq()).thenReturn(KDLQConfiguration.DLQ.builder().build("test"));

        return configuration;
    }

    private KDLQProducerRecord<byte[], byte[]> createMockRecord(long nextRedeliveryTimestamp) {
        final var record = mock(KDLQProducerRecord.class);
        when(record.nextRedeliveryTimestamp()).thenReturn(nextRedeliveryTimestamp);
        when(record.configuration()).thenReturn(this.configuration);

        final var kafkaRecord = mock(ProducerRecord.class);
        when(kafkaRecord.topic()).thenReturn("test");

        final var headers = mock(Headers.class);
        when(headers.lastHeader(eq(MESSAGE_PARTITION_HEADER)))
                .thenReturn(this.headersService.createIntHeader(MESSAGE_PARTITION_HEADER, (int) (nextRedeliveryTimestamp % 2)));
        when(kafkaRecord.headers()).thenReturn(headers);

        when(record.record()).thenReturn(kafkaRecord);

        return record;
    }
}
