package io.millesabords.pulsar.io.mongo;

import java.util.HashMap;
import java.util.Map;

public final class TestHelper {

    public static final String URI = "mongodb://localhost";

    public static final String DB = "pulsar";

    public static final String COLL = "messages";

    public static final int BATCH_SIZE = 2;

    public static final int BATCH_TIME = 500;


    public static Map<String, Object> createMap(boolean full) {
        final Map<String, Object> map = new HashMap<>();
        map.put("mongoUri", URI);
        map.put("database", DB);

        if (full) {
            map.put("collection", COLL);
            map.put("batchSize", BATCH_SIZE);
            map.put("batchTimeMs", BATCH_TIME);
        }

        return map;
    }

    private TestHelper() {

    }
}