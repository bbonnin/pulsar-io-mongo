package io.millesabords.pulsar.io.mongo;

import java.util.HashMap;
import java.util.Map;

public final class TestHelper {

    public static final String URI = "mongodb://localhost";

    public static final String DB = "pulsar";

    public static final String COLL = "messages";

    public static Map<String, Object> createMap(boolean full) {
        final Map<String, Object> map = new HashMap<>();
        map.put("mongoUri", URI);
        map.put("database", DB);

        if (full) {
            map.put("collection", COLL);
        }

        return map;
    }

    private TestHelper() {

    }
}
