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
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducer;
import ru.joke.kdlq.internal.routers.producers.KDLQMessageProducerFactory;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

    private final KDLQGlobalConfiguration globalConfiguration;
    private final KDLQConfigurationRegistry configurationRegistry;
    private final KDLQMessageProducerFactory senderFactory;
    private volatile Future<?> future;

    /**
     * Constructs the redelivery task.
     *
     * @param globalConfiguration   KDLQ global configuration; cannot be {@code null}.
     * @param configurationRegistry configuration registry; cannot be {@code null}.
     * @param senderFactory         factory of the message senders; cannot be {@code null}.
     */
    public RedeliveryTask(
            @Nonnull final KDLQGlobalConfiguration globalConfiguration,
            @Nonnull final KDLQConfigurationRegistry configurationRegistry,
            @Nonnull final KDLQMessageProducerFactory senderFactory
    ) {
        this.globalConfiguration = globalConfiguration;
        this.configurationRegistry = configurationRegistry;
        this.senderFactory = senderFactory;
    }

    @Override
    public void run() {
        final var lockService = this.globalConfiguration.distributedLockService();
        if (!lockService.tryLock()) {
            scheduleNextRedelivery();
            return;
        }

        try {
            redeliver();
        } catch (RuntimeException ex) {
            if (this.future.isCancelled()) {
                return;
            }

            logger.error("Exception while redelivering", ex);
        } finally {
            lockService.releaseLock();
        }

        scheduleNextRedelivery();
    }

    private void scheduleNextRedelivery() {
        final var redeliveryPool = this.globalConfiguration.redeliveryPool();
        this.future = redeliveryPool.schedule(
                this,
                this.globalConfiguration.redeliveryTaskDelay(),
                TimeUnit.MILLISECONDS
        );
    }

    private void redeliver() {
        final Map<String, KDLQConfiguration> redeliveryConfigs = new HashMap<>();
        final Map<String, KDLQMessageProducer<byte[], byte[]>> senders = new HashMap<>();

        final var storage = this.globalConfiguration.redeliveryStorage();
        try {
            storage.findAllReadyToRedelivery(this::findConfigById, System.currentTimeMillis())
                    .forEach(record -> redeliver(senders, redeliveryConfigs, record));
        } finally {
            senders.values().forEach(KDLQMessageProducer::close);
        }
    }

    private KDLQConfiguration findConfigById(final String configId) {
        return this.configurationRegistry.get(configId).orElse(null);
    }

    private void redeliver(
            final Map<String, KDLQMessageProducer<byte[], byte[]>> senders,
            final Map<String, KDLQConfiguration> configs,
            final KDLQProducerRecord.Identifiable<byte[], byte[]> record
    ) {
        if (this.future.isCancelled()) {
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
            if (this.future.isCancelled()) {
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
        final var header = record.headers().lastHeader(MESSAGE_PRC_MARKER_HEADER);
        if (header == null) {
            return REDELIVERY_PROCESSOR;
        }

        final var headerBytes = header.value();
        return new String(headerBytes, StandardCharsets.UTF_8);
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
        this.future.cancel(true);
    }
}
