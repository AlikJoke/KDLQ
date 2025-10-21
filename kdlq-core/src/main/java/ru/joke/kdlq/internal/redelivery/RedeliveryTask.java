package ru.joke.kdlq.internal.redelivery;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQGlobalConfiguration;
import ru.joke.kdlq.internal.configs.KDLQConfigurationRegistry;
import ru.joke.kdlq.internal.routers.KDLQMessageSender;
import ru.joke.kdlq.internal.routers.KDLQMessageSenderFactory;
import ru.joke.kdlq.KDLQProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class RedeliveryTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RedeliveryTask.class);

    private final KDLQGlobalConfiguration globalConfiguration;
    private final KDLQConfigurationRegistry configurationRegistry;
    private final KDLQMessageSenderFactory senderFactory;

    public RedeliveryTask(
            final KDLQGlobalConfiguration globalConfiguration,
            final KDLQConfigurationRegistry configurationRegistry,
            final KDLQMessageSenderFactory senderFactory
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
        } finally {
            lockService.releaseLock();
        }

        scheduleNextRedelivery();
    }

    private void scheduleNextRedelivery() {
        this.globalConfiguration.redeliveryPool().schedule(this, globalConfiguration.redeliveryTaskDelay(), TimeUnit.MILLISECONDS);
    }

    private void redeliver() {
        final Map<String, KDLQConfiguration> configsToSend = new HashMap<>();
        final Map<String, KDLQMessageSender<byte[], byte[]>> sendersMap = new HashMap<>();

        final var storage = this.globalConfiguration.redeliveryStorage();
        try {
            storage.findAllReadyToRedelivery(this::findConfigById, System.currentTimeMillis())
                    .forEach(record -> redeliver(sendersMap, configsToSend, record));
        } finally {
            sendersMap.values().forEach(KDLQMessageSender::close);
        }
    }

    private KDLQConfiguration findConfigById(final String configId) {
        return this.configurationRegistry.get(configId).orElse(null);
    }

    private void redeliver(
            final Map<String, KDLQMessageSender<byte[], byte[]>> senders,
            final Map<String, KDLQConfiguration> configs,
            final KDLQProducerRecord<byte[], byte[]> record
    ) {
        final var originalConfig = record.configuration();
        if (originalConfig == null) {
            return;
        }

        final var configToSend = configs.computeIfAbsent(
                originalConfig.id(),
                k -> buildConfigurationToSend(record.configuration())
        );

        final var sender = senders.computeIfAbsent(
                configToSend.producerId(),
                k -> this.senderFactory.create(configToSend)
        );

        final var lifecycleListeners = configToSend.lifecycleListeners();
        final var sourceProcessorId = new String(record.record().headers().lastHeader("KDLQ_PrcMarker").value(), StandardCharsets.UTF_8);
        try {
            sender.send(record.record());
        } catch (Exception ex) {
            logger.error("Unable to redeliver stored message: " + record.id(), ex);
            lifecycleListeners.forEach(l -> l.onMessageRedeliveryError(sourceProcessorId, Optional.empty(), record.record(), ex));

            return;
        }

        lifecycleListeners.forEach(l -> l.onMessageRedeliverySuccess(sourceProcessorId, Optional.empty(), record.record()));
        this.globalConfiguration.redeliveryStorage().deleteById(record.id());
    }

    private KDLQConfiguration buildConfigurationToSend(final KDLQConfiguration original) {
        final Map<String, Object> producerProperties = new HashMap<>(original.producerProperties());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return KDLQConfiguration.builder()
                                    .withId(original.id())
                                    .withProducerProperties(producerProperties)
                                    .withLifecycleListeners(original.lifecycleListeners())
                                    .withRedelivery(original.redelivery())
                                .build(original.bootstrapServers(), original.dlq());
    }
}
