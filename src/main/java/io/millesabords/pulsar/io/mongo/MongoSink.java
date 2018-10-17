package io.millesabords.pulsar.io.mongo;

import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.bson.BSONException;
import org.bson.Document;
import org.bson.json.JsonParseException;

import java.util.Map;

@Slf4j
public class MongoSink implements Sink<byte[]> {

    private MongoConfig mongoConfig;

    private MongoClient mongoClient;


    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        mongoConfig = MongoConfig.load(config);
        mongoConfig.validate();

        mongoClient = MongoClients.create(mongoConfig.getMongoUri());
    }

    @Override
    public void write(Record<byte[]> record) {
        log.info("Value is " + new String(record.getValue()));
        try {
            final byte[] docAsBytes = record.getValue();
            final Document doc = Document.parse(new String(docAsBytes));
            final MongoDatabase db = mongoClient.getDatabase(mongoConfig.getDatabase());
            final MongoCollection<Document> collection = db.getCollection(mongoConfig.getCollection());

            collection.insertOne(doc, (Void result, final Throwable t) -> {
                if (t == null) {
                    record.ack();
                } else {
                    log.error("MongoDB insertion error", t);
                    record.fail();
                }
            });
        }
        catch (JsonParseException | BSONException e) {
            log.error("Bad message", e);
            record.fail();
        }
    }

    @Override
    public void close() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
