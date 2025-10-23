package ru.joke.kdlq.spi;

import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQGlobalConfiguration;
import ru.joke.kdlq.KDLQProducerRecord;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * A message store for messages scheduled for redelivery.<br>
 * This storage will only be used for messages received by consumers whose configuration
 * implies the use of delayed redelivery {@link KDLQConfiguration.Redelivery#redeliveryDelay()}.<br>
 * For reliable redelivery operation in a distributed system, the storage implementation must be
 * based on a persistent storage that is shared across all KDLQ components of the distributed system.<br>
 * For a standalone system, only the persistence property is sufficient; distributed capabilities
 * are only required when using a cluster.
 * @implSpec Implementations must be thread-safe.
 *
 * @author Alik
 * @see KDLQGlobalConfiguration#redeliveryStorage()
 * @see ru.joke.kdlq.KDLQGlobalConfigurationFactory
 */
public interface KDLQRedeliveryStorage {

    /**
     * Stores the message for redelivery in the persistence store.
     * @implSpec If the write operation fails for any reason, an exception must be thrown.
     *
     * @param obj message to store; cannot be {@code null}.
     * @see KDLQProducerRecord
     */
    void store(@Nonnull KDLQProducerRecord<byte[], byte[]> obj);

    /**
     * Deletes the message by its id from the persistence store.
     *
     * @param objId message id; cannot be {@code null} or empty.
     * @see #deleteByIds(Set)
     */
    default void deleteById(@Nonnull String objId) {
        deleteByIds(Set.of(objId));
    }

    /**
     * Deletes the messages by their ids from the persistence store.
     *
     * @param objectIds message ids; cannot be {@code null}.
     * @see #deleteById(String)
     */
    void deleteByIds(@Nonnull Set<String> objectIds);

    /**
     * Finds all messages ready for redelivery whose scheduled redelivery time is either equal
     * to or before the given timestamp (in milliseconds since the epoch).<br>
     * Message keys and bodies must be in the form of byte arrays.
     *
     * @param configurationFactory a factory for retrieving a configuration object based on the
     *                             configuration id stored (at the time the message was saved)
     *                             in the store; cannot be {@code null}.
     * @param redeliveryTimestamp  given timestamp in milliseconds since the epoch; cannot be negative.
     * @param limit                max number of messages for selection; cannot be negative.
     * @return all messages ready for redelivery; cannot be {@code null}.
     */
    @Nonnull
    List<KDLQProducerRecord.Identifiable<byte[], byte[]>> findAllReadyToRedelivery(
            @Nonnull Function<String, KDLQConfiguration> configurationFactory,
            @Nonnegative long redeliveryTimestamp,
            @Nonnegative int limit
    );
}
