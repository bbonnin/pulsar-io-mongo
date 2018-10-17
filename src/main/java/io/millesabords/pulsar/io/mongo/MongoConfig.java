package io.millesabords.pulsar.io.mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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

    private String mongoUri;

    private String database;

    private String collection;


    public static MongoConfig load(String yamlFile) throws IOException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final MongoConfig cfg = mapper.readValue(new File(yamlFile), MongoConfig.class);

        return cfg;
    }

    public static MongoConfig load(Map<String, Object> map) throws IOException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final MongoConfig cfg = mapper.readValue(new ObjectMapper().writeValueAsString(map), MongoConfig.class);

        return cfg;
    }

    public void validate() {
        if (StringUtils.isEmpty(mongoUri) || StringUtils.isEmpty(database) || StringUtils.isEmpty(collection)) {
            throw new IllegalArgumentException("Required property not set.");
        }
    }
}
