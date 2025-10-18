package ru.joke.kdlq.mongo.internal;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import ru.joke.kdlq.KDLQConfiguration;
import ru.joke.kdlq.spi.KDLQProducerRecord;
import ru.joke.kdlq.spi.KDLQRedeliveryStorage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Indexes.ascending;

public final class KDLQRedeliveryMongoStorage implements KDLQRedeliveryStorage {

    private static final String REDELIVERY_TS_FIELD = "rd_ts";

    private final MongoCollection<Document> collection;

    public KDLQRedeliveryMongoStorage(final MongoCollection<Document> collection) {
        this.collection = collection;
        createIndexesIfNeed(collection);
    }

    @Override
    public void store(@Nonnull KDLQProducerRecord<?, ?> message) {

        final var record = message.record();
        final Document messageDoc =
                new Document()
                        .append("_id", message.id())
                        .append("ts", record.timestamp())
                        .append("rd_ts", message.nextRedeliveryTimestamp())
                        .append("tpc", record.topic())
                        .append("cfg", message.configuration().id())
                        .append("prt", record.partition());

        final var redeliveryConfig = message.configuration().redelivery();
        if (record.key() != null) {
            messageDoc.append("ky", new Binary(redeliveryConfig.messageKeyConverter().toByteArray(record.key())));
        }

        messageDoc.append("val", new Binary(redeliveryConfig.messageBodyConverter().toByteArray(record.value())));

        final var headersDoc = new Document();
        record.headers().forEach(h -> headersDoc.append(h.key(), new Binary(h.value())));

        if (!headersDoc.isEmpty()) {
            messageDoc.append("hdrs", headersDoc);
        }

        this.collection.insertOne(messageDoc);
    }

    @Nonnull
    @Override
    public List<KDLQProducerRecord<?, ?>> findAllReadyToRedelivery(
            @Nonnull Function<String, KDLQConfiguration> configurationLoader,
            @Nonnegative final long timestamp
    ) {
        // TODO
        return this.collection.find(Filters.lte(REDELIVERY_TS_FIELD, timestamp))
                .map(d -> new KDLQProducerRecord<>() {
                    @Override
                    public String id() {
                        return null;
                    }

                    @Override
                    public ProducerRecord<Object, Object> record() {
                        return null;
                    }

                    @Override
                    public KDLQConfiguration configuration() {
                        // TODO
                        return configurationLoader.apply(null);
                    }

                    @Override
                    public long nextRedeliveryTimestamp() {
                        return 0;
                    }
                })
                .into(new ArrayList<>());
    }

    @Override
    public void deleteById(@Nonnull String objectId) {
        this.collection.deleteOne(Filters.eq("_id", objectId));
    }

    @Override
    public void deleteByIds(@Nonnull Set<String> objectIds) {
        final var ids = objectIds.stream().map(ObjectId::new).collect(Collectors.toSet());
        this.collection.deleteMany(Filters.in("_id", ids));
    }

    private void createIndexesIfNeed(final MongoCollection<Document> collection) {
        try {
            final var indexOptions = new IndexOptions().name(REDELIVERY_TS_FIELD).unique(false);
            collection.createIndex(ascending(REDELIVERY_TS_FIELD), indexOptions);
        } catch (MongoWriteException ignored) {
        }
    }
}
