package ru.joke.kdlq.internal.routers.headers;

/**
 * Custom header constants for re-sent KDLQ messages.
 *
 * @author Alik
 */
public abstract class KDLQHeaders {

    /**
     * KDLQ kills (DLQ send counter) counter.
     */
    public static final String MESSAGE_KILLS_HEADER = "KDLQ_Kills";

    /**
     * KDLQ header with marker identifying the KDLQ handler that sent the message
     * for redelivery or to the DLQ.
     */
    public static final String MESSAGE_PRC_MARKER_HEADER = "KDLQ_PrcMarker";

    /**
     * KDLQ header with the original message's creation timestamp.
     */
    public static final String MESSAGE_TIMESTAMP_HEADER = "KDLQ_OrigTs";

    /**
     * KDLQ header with the original message's offset.
     */
    public static final String MESSAGE_OFFSET_HEADER = "KDLQ_OrigOffset";

    /**
     * KDLQ header with the original message's partition number.
     */
    public static final String MESSAGE_PARTITION_HEADER = "KDLQ_OrigPartition";

    /**
     * KDLQ header with the message's redelivery counter.
     */
    public static final String MESSAGE_REDELIVERY_ATTEMPTS_HEADER = "KDLQ_Redelivered";

    private KDLQHeaders() {}
}
