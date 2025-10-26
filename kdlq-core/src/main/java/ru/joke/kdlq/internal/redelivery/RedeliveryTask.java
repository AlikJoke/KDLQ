package ru.joke.kdlq.internal.redelivery;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQException;
import ru.joke.kdlq.KDLQGlobalConfiguration;
import ru.joke.kdlq.KDLQProducerRecord;
import ru.joke.kdlq.internal.configs.KDLQConfigurationRegistry;
import ru.joke.kdlq.internal.routers.headers.KDLQHeaders;
import ru.joke.kdlq.internal.routers.headers.KDLQHeadersService;
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducer;
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducerFactory;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static ru.joke.kdlq.internal.routers.headers.KDLQHeaders.MESSAGE_PARTITION_HEADER;
import static ru.joke.kdlq.internal.routers.headers.KDLQHeaders.MESSAGE_PRC_MARKER_HEADER;

/**
 * A task for delayed redelivery of messages previously saved in the redelivered message storage.<br>
 * For proper operation, a distributed locking service is required to synchronize task execution across
 * different nodes in a distributed system, thereby preventing duplicate message sending.
 *
 * @author Alik
 * @see KDLQGlobalConfiguration
 * @see ru.joke.kdlq.spi.KDLQGlobalDistributedLockService
 * @see ru.joke.kdlq.spi.KDLQRedeliveryStorage
 */
public final class RedeliveryTask implements Runnable, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(RedeliveryTask.class);
    private static final String REDELIVERY_PROCESSOR = "KDLQ_Redelivery";
    private static final int BATCH_SIZE = 2_000;

    private final KDLQGlobalConfiguration globalConfiguration;
    private final KDLQConfigurationRegistry configurationRegistry;
    private final KDLQMessageProducerFactory senderFactory;
    private final KDLQHeadersService headersService;
    private volatile Future<?> dispatcherFuture;
    private volatile List<Future<?>> redeliveryFutures;

    /**
     * Constructs the redelivery task.
     *
     * @param globalConfiguration   KDLQ global configuration; cannot be {@code null}.
     * @param configurationRegistry configuration registry; cannot be {@code null}.
     * @param senderFactory         factory of the message senders; cannot be {@code null}.
     * @param headersService        KDLQ message headers service; cannot be {@code null}.
     */
    public RedeliveryTask(
            @Nonnull final KDLQGlobalConfiguration globalConfiguration,
            @Nonnull final KDLQConfigurationRegistry configurationRegistry,
            @Nonnull final KDLQMessageProducerFactory senderFactory,
            @Nonnull final KDLQHeadersService headersService
    ) {
        this.globalConfiguration = globalConfiguration;
        this.configurationRegistry = configurationRegistry;
        this.senderFactory = senderFactory;
        this.headersService = headersService;
    }

    @Override
    public void run() {
        final var lockService = this.globalConfiguration.distributedLockService();
        if (this.dispatcherFuture == null || !lockService.tryLock()) {
            scheduleNextRedelivery();
            return;
        }

        try {
            redeliver();
        } catch (RuntimeException ex) {
            if (this.dispatcherFuture.isCancelled()) {
                return;
            }

            logger.error("Exception while redelivering", ex);
        } finally {
            lockService.releaseLock();
        }

        scheduleNextRedelivery();
    }

    private void scheduleNextRedelivery() {
        final var redeliveryDispatcherPool = this.globalConfiguration.redeliveryDispatcherPool();
        this.dispatcherFuture = redeliveryDispatcherPool.schedule(
                this,
                this.globalConfiguration.redeliveryDispatcherTaskDelay(),
                TimeUnit.MILLISECONDS
        );
    }

    private void redeliver() {
        final Map<String, KDLQConfiguration> redeliveryConfigs = new HashMap<>();
        final Map<String, KDLQMessageProducer<byte[], byte[]>> senders = new HashMap<>();

        final var storage = this.globalConfiguration.redeliveryStorage();
        try {
            List<KDLQProducerRecord.Identifiable<byte[], byte[]>> batch = null;
            do {
                batch = storage.findAllReadyToRedelivery(this::findConfigById, System.currentTimeMillis(), BATCH_SIZE);
                redeliver(batch, redeliveryConfigs, senders);
            } while (!this.dispatcherFuture.isCancelled() && !batch.isEmpty());
        } finally {
            senders.values().forEach(KDLQMessageProducer::close);
        }
    }

    private void redeliver(
            final List<KDLQProducerRecord.Identifiable<byte[], byte[]>> messages,
            final Map<String, KDLQConfiguration> redeliveryConfigs,
            final Map<String, KDLQMessageProducer<byte[], byte[]>> senders
    ) {
        final var dividedMessages = divide(messages);

        this.redeliveryFutures =
                dividedMessages
                        .stream()
                        .map(messagesPart -> createRedeliveryTask(messagesPart, redeliveryConfigs, senders))
                        .map(this.globalConfiguration.redeliveryPool()::submit)
                        .collect(Collectors.toList());

        this.redeliveryFutures.forEach(this::await);
    }

    private void await(final Future<?> future) {
        if (future.isCancelled()) {
            return;
        }

        try {
            future.get(1, TimeUnit.MINUTES);
        } catch (ExecutionException | TimeoutException e) {
            logger.error("", e);
        } catch (InterruptedException e) {
            logger.debug("Redelivery task thread was interrupted");
            Thread.currentThread().interrupt();
        }
    }

    private Runnable createRedeliveryTask(
            final List<KDLQProducerRecord.Identifiable<byte[], byte[]>> messages,
            final Map<String, KDLQConfiguration> redeliveryConfigs,
            final Map<String, KDLQMessageProducer<byte[], byte[]>> senders
    ) {
        return () -> messages.forEach(record -> redeliver(senders, redeliveryConfigs, record));
    }

    private Collection<List<KDLQProducerRecord.Identifiable<byte[], byte[]>>> divide(final List<KDLQProducerRecord.Identifiable<byte[], byte[]>> messages) {
        final Map<String, List<KDLQProducerRecord.Identifiable<byte[], byte[]>>> messagesByKey = new HashMap<>();
        messages.forEach(m -> {
            final var topic = m.record().topic();
            final var partition = this.headersService.getIntHeader(MESSAGE_PARTITION_HEADER, m.record().headers()).orElse(0);

            final var key = topic + "%" + partition;
            messagesByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(m);
        });

        return messagesByKey.values();
    }

    private KDLQConfiguration findConfigById(final String configId) {
        return this.configurationRegistry.get(configId).orElse(null);
    }

    private void redeliver(
            final Map<String, KDLQMessageProducer<byte[], byte[]>> senders,
            final Map<String, KDLQConfiguration> configs,
            final KDLQProducerRecord.Identifiable<byte[], byte[]> record
    ) {
        if (this.dispatcherFuture.isCancelled()) {
            throw new KDLQException("Task was cancelled");
        }

        final var originalConfig = record.configuration();
        if (originalConfig == null) {
            logger.trace("Config not found for message {}", record.id());
            return;
        }

        final var redeliveryConfig = configs.computeIfAbsent(
                originalConfig.id(),
                k -> buildRedeliveryConfiguration(originalConfig)
        );

        final var sender = senders.computeIfAbsent(
                redeliveryConfig.producerId(),
                k -> this.senderFactory.create(redeliveryConfig)
        );

        final var lifecycleListeners = redeliveryConfig.lifecycleListeners();
        final var sourceProcessorId = extractProcessorId(record.record());

        try {
            sender.send(record.record());
        } catch (Exception ex) {
            if (this.dispatcherFuture.isCancelled()) {
                return;
            }

            logger.error("Unable to redeliver stored message: " + record.id(), ex);
            lifecycleListeners.forEach(
                    l -> l.onMessageRedeliveryError(
                            sourceProcessorId,
                            Optional.empty(),
                            record.record(),
                            ex
                    )
            );

            return;
        }

        lifecycleListeners.forEach(
                l -> l.onMessageRedeliverySuccess(
                        sourceProcessorId,
                        Optional.empty(),
                        record.record()
                )
        );

        this.globalConfiguration.redeliveryStorage().deleteById(record.id());
    }

    private String extractProcessorId(final ProducerRecord<byte[], byte[]> record) {
        return this.headersService.getStringHeader(MESSAGE_PRC_MARKER_HEADER, record.headers()).orElse(REDELIVERY_PROCESSOR);
    }

    private KDLQConfiguration buildRedeliveryConfiguration(final KDLQConfiguration original) {
        final Map<String, Object> producerProperties = new HashMap<>(original.producerProperties());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return KDLQConfiguration.builder()
                                    .withId(original.id())
                                    .withLifecycleListeners(original.lifecycleListeners())
                                    .withRedelivery(original.redelivery())
                                .build(original.bootstrapServers(), producerProperties, original.dlq());
    }

    @Override
    public void close() {
        if (this.dispatcherFuture != null) {
            this.dispatcherFuture.cancel(true);
        }

        if (this.redeliveryFutures != null) {
            this.redeliveryFutures.forEach(f -> f.cancel(true));
        }
    }
}
