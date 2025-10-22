package ru.joke.kdlq.internal.routers;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nonnull;
import java.io.Closeable;

/**
 * Message router that direct messages to redelivery or the DLQ based on KDLQ configuration.
 *
 * @param <K> type of message key
 * @param <V> type of message body
 * @author Alik
 * @see KDLQMessageRouterFactory
 */
public sealed interface KDLQMessageRouter<K, V> extends Closeable permits InternalKDLQMessageRouter {

    /**
     * Routes message to redelivery (direct in redelivery queue or in redelivery
     * storage if redelivery delay is configured).
     *
     * @param originalMessage message to routing; cannot be {@code null}.
     * @return status of redelivery routing; cannot be {@code null}.
     * @see RoutingStatus
     */
    @Nonnull
    RoutingStatus routeToRedelivery(@Nonnull ConsumerRecord<K, V> originalMessage);

    /**
     * Routes message to DLQ.
     *
     * @param originalMessage message to routing; cannot be {@code null}.
     * @return status of routing; cannot be {@code null}.
     * @see RoutingStatus
     */
    @Nonnull
    RoutingStatus routeToDLQ(@Nonnull ConsumerRecord<K, V> originalMessage);

    @Override
    void close();

    /**
     * Enumeration of possible routing statuses.
     */
    enum RoutingStatus {

        /**
         * Routed directly to redelivery queue.
         */
        ROUTED_TO_REDELIVERY_QUEUE,
        /**
         * Routed to redelivery storage and scheduled to next redelivery
         * due to redelivery delay.
         */
        SCHEDULED_TO_REDELIVERY,
        /**
         * Routed directly to DLQ.
         */
        ROUTED_TO_DLQ,
        /**
         * Message was not redirected to the DLQ because the number of attempts to
         * resubmit to the DLQ exceeded the configured value.
         */
        DISCARDED
    }
}
