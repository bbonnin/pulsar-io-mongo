package io.millesabords.pulsar.io.mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import lombok.*;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Configuration class for the MongoDB Sink Connector.
 */
@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class MongoConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_BATCH_SIZE = 100;

    public static final long DEFAULT_BATCH_TIME_MS = 1000;

    /*@FieldDoc(
            required = true,
            defaultValue = "",
            help = "The uri of mongodb that the connector connects to" +
                    " (see: https://docs.mongodb.com/manual/reference/connection-string/)"
    )*/
    private String mongoUri;

    /*@FieldDoc(
            required = true,
            defaultValue = "",
            help = "The name of the database to which the collection belongs to"
    )*/
    private String database;

    /*@FieldDoc(
            required = true,
            defaultValue = "",
            help = "The collection name that the connector writes messages to"
    )*/
    private String collection;

    /*@FieldDoc(
            required = false,
            defaultValue = "" + DEFAULT_BATCH_SIZE,
            help = "The batch size of write to the collection"
    )*/
    private int batchSize = DEFAULT_BATCH_SIZE;

    /*@FieldDoc(
            required = false,
            defaultValue = "" + DEFAULT_BATCH_TIME_MS,
            help = "The batch operation interval in milliseconds"
    )*/
    private long batchTimeMs = DEFAULT_BATCH_TIME_MS;


    public static MongoConfig load(String yamlFile) throws IOException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final MongoConfig cfg = mapper.readValue(new File(yamlFile), MongoConfig.class);

        return cfg;
    }

    public static MongoConfig load(Map<String, Object> map) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final MongoConfig cfg = mapper.readValue(new ObjectMapper().writeValueAsString(map), MongoConfig.class);

        return cfg;
    }

    public void validate() {
        if (StringUtils.isEmpty(mongoUri) || StringUtils.isEmpty(database) || StringUtils.isEmpty(collection)) {
            throw new IllegalArgumentException("Required property not set.");
        }

        Preconditions.checkArgument(batchSize > 0, "batchSize must be a positive integer.");
        Preconditions.checkArgument(batchTimeMs > 0, "batchTimeMs must be a positive long.");
    }
}
