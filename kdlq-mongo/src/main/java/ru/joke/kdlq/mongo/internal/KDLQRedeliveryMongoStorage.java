package ru.joke.kdlq.mongo.internal;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.KDLQConfigurationException;
import ru.joke.kdlq.KDLQException;
import ru.joke.kdlq.KDLQProducerRecord;
import ru.joke.kdlq.internal.util.Args;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Indexes.ascending;

/**
 * MongoDB-based implementation of the {@link KDLQRedeliveryStorage}.
 *
 * @author Alik
 * @see KDLQRedeliveryStorage
 */
@ThreadSafe
public final class KDLQRedeliveryMongoStorage implements KDLQRedeliveryStorage {

    private static final String REDELIVERY_TIMESTAMP_FIELD = "rd_ts";
    private static final String ID_FIELD = "_id";
    private static final String MSG_TIMESTAMP_FIELD = "ts";
    private static final String TOPIC_FIELD = "tpc";
    private static final String CONFIG_ID_FIELD = "cfg";
    private static final String PARTITION_FIELD = "prt";
    private static final String MSG_KEY_FIELD = "k";
    private static final String MSG_BODY_FIELD = "v";
    private static final String MSG_HEADERS_FIELD = "hs";

    private final MongoCollection<Document> collection;

    /**
     * Constructs redelivery storage.
     *
     * @param collection collection with messages to redelivery; cannot be {@code null}.
     */
    public KDLQRedeliveryMongoStorage(@Nonnull final MongoCollection<Document> collection) {
        this.collection = Args.requireNotNull(collection, () -> new KDLQConfigurationException("Provided collection must be not null"));
        createIndexesIfNeed(collection);
    }

    @Override
    public void store(@Nonnull KDLQProducerRecord<byte[], byte[]> message) {

        final var config = message.configuration();
        if (config == null) {
            throw new KDLQException("Configuration must be not null");
        }

        final var record = message.record();
        final Document messageDoc =
                new Document()
                        .append(MSG_TIMESTAMP_FIELD, record.timestamp())
                        .append(REDELIVERY_TIMESTAMP_FIELD, message.nextRedeliveryTimestamp())
                        .append(TOPIC_FIELD, record.topic())
                        .append(CONFIG_ID_FIELD, config.id());

        if (record.key() != null) {
            messageDoc.append(MSG_KEY_FIELD, new Binary(record.key()));
        }

        messageDoc.append(MSG_BODY_FIELD, new Binary(record.value()));

        final var headersDoc = new Document();
        record.headers().forEach(h -> headersDoc.append(h.key(), new Binary(h.value())));

        if (!headersDoc.isEmpty()) {
            messageDoc.append(MSG_HEADERS_FIELD, headersDoc);
        }

        this.collection.insertOne(messageDoc);
    }

    @Nonnull
    @Override
    public List<KDLQProducerRecord.Identifiable<byte[], byte[]>> findAllReadyToRedelivery(
            @Nonnull final Function<String, KDLQConfiguration> configurationLoader,
            @Nonnegative final long timestamp
    ) {
        final var result = this.collection.find(Filters.lte(REDELIVERY_TIMESTAMP_FIELD, timestamp));
        return result
                .map(d -> mapDocumentToRecord(d, configurationLoader))
                .into(new ArrayList<>());
    }

    @Override
    public void deleteById(@Nonnull String objectId) {
        this.collection.deleteOne(Filters.eq(ID_FIELD, objectId));
    }

    @Override
    public void deleteByIds(@Nonnull Set<String> objectIds) {
        if (objectIds.isEmpty()) {
            return;
        }

        final var ids =
                objectIds
                        .stream()
                        .map(ObjectId::new)
                        .collect(Collectors.toSet());
        this.collection.deleteMany(Filters.in(ID_FIELD, ids));
    }

    private KDLQProducerRecord.Identifiable<byte[], byte[]> mapDocumentToRecord(
            final Document doc,
            final Function<String, KDLQConfiguration> configurationLoader
    ) {

        final var id = doc.getObjectId(ID_FIELD).toString();
        final var cfgId = doc.getString(CONFIG_ID_FIELD);
        final var nextRedeliveryTimestamp = (long) doc.get(REDELIVERY_TIMESTAMP_FIELD, 0L);
        final var producerRecord = createProducerRecordFromDoc(doc);

        return new KDLQProducerRecord.Identifiable<>() {
            @Override
            @Nonnull
            public String id() {
                return id;
            }

            @Override
            @Nonnull
            public ProducerRecord<byte[], byte[]> record() {
                return producerRecord;
            }

            @Override
            public KDLQConfiguration configuration() {
                return configurationLoader.apply(cfgId);
            }

            @Override
            public long nextRedeliveryTimestamp() {
                return nextRedeliveryTimestamp;
            }
        };
    }

    private ProducerRecord<byte[], byte[]> createProducerRecordFromDoc(final Document doc) {
        final var topic = doc.getString(TOPIC_FIELD);
        final var key = doc.get(MSG_KEY_FIELD, Binary.class);
        final var body = doc.get(MSG_BODY_FIELD, Binary.class);
        final var headersDoc = doc.get(MSG_HEADERS_FIELD, Document.class);

        final List<Header> headers =
                headersDoc == null || headersDoc.isEmpty()
                        ? Collections.emptyList()
                        : extractHeaders(headersDoc);

        return new ProducerRecord<>(
                topic,
                null,
                key == null ? null : key.getData(),
                body == null ? null : body.getData(),
                headers
        );
    }

    private List<Header> extractHeaders(final Document headersDoc) {
        return headersDoc.keySet()
                            .stream()
                            .map(headerName -> {
                                final var headerValue = headersDoc.get(headerName, Binary.class);
                                return new RecordHeader(headerName, headerValue == null ? new byte[0] : headerValue.getData());
                            })
                            .collect(Collectors.toList());
    }

    private void createIndexesIfNeed(final MongoCollection<Document> collection) {
        try {
            final var indexOptions = new IndexOptions().name(REDELIVERY_TIMESTAMP_FIELD).unique(false);
            collection.createIndex(ascending(REDELIVERY_TIMESTAMP_FIELD), indexOptions);
        } catch (MongoWriteException ignored) {
        }
    }
}
