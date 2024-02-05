package ru.joke.kdlq.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;

public interface KDLQMessageLifecycleListener {

    default <K, V> void onMessageKillSuccess(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> dlqMessage) {
    }

    default <K, V> void onMessageKillError(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> dlqMessage,
            @Nonnull Exception error) {
    }

    default <K, V> void onMessageRedeliverySuccess(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> messageToRedelivery) {
    }

    default <K, V> void onMessageRedeliveryError(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> originalMessage,
            @Nonnull ProducerRecord<K, V> messageToRedelivery,
            @Nonnull RuntimeException error) {
    }

    default <K, V> void onMessageSkip(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> message) {
    }

    default <K, V> void onSuccessMessageProcessing(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> message,
            @Nonnull KDLQMessageProcessor.ProcessingStatus processingStatus) {
    }

    default <K, V> void onErrorMessageProcessing(
            @Nonnull String consumerId,
            @Nonnull ConsumerRecord<K, V> message,
            @Nonnull KDLQMessageProcessor.ProcessingStatus processingStatus,
            @Nonnull RuntimeException error) {
    }
}
