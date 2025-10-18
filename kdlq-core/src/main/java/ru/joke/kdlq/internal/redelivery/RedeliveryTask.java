package ru.joke.kdlq.internal.redelivery;

import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQGlobalConfiguration;
import ru.joke.kdlq.internal.configs.KDLQConfigurationRegistry;
import ru.joke.kdlq.internal.routers.KDLQMessageSender;
import ru.joke.kdlq.internal.routers.KDLQMessageSenderFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class RedeliveryTask implements Runnable {

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
                        Collectors.toMap(KDLQConfiguration::id, this.senderFactory::create));

        globalConfiguration.redeliveryStorage().findAllReadyToRedelivery(cfgId -> this.configurationRegistry.get(cfgId).orElse(null), System.currentTimeMillis()).forEach(record -> {
            @SuppressWarnings("rawtypes")
            final KDLQMessageSender sender = sendersMap.get(record.configuration().id());
            sender.send(record.record());
            globalConfiguration.redeliveryStorage().deleteById(record.id());
        });
    }
}
