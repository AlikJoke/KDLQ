package ru.joke.kdlq.internal.redelivery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQGlobalConfiguration;
import ru.joke.kdlq.internal.configs.KDLQConfigurationRegistry;
import ru.joke.kdlq.internal.routers.KDLQMessageSender;
import ru.joke.kdlq.internal.routers.KDLQMessageSenderFactory;
import ru.joke.kdlq.spi.KDLQProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
        final Map<String, KDLQMessageSender<?, ?>> sendersMap =
                this.configurationRegistry.getAll().stream().collect(
                        Collectors.toMap(KDLQConfiguration::producerId, this.senderFactory::create));

        globalConfiguration.redeliveryStorage().findAllReadyToRedelivery(cfgId -> this.configurationRegistry.get(cfgId).orElse(null), System.currentTimeMillis())
                .forEach(record -> redeliver(sendersMap, record));
    }

    @SuppressWarnings("unchecked")
    private void redeliver(
            final Map<String, KDLQMessageSender<?, ?>> senders,
            final KDLQProducerRecord<?, ?> record
    ) {
        @SuppressWarnings("rawtypes")
        final KDLQMessageSender sender = senders.get(record.configuration().producerId());
        if (sender == null) {
            return;
        }

        final var lifecycleListeners = record.configuration().lifecycleListeners();
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
}
