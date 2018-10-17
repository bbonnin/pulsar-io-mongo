package io.millesabords.pulsar.io.mongo;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class MongoConfigTest {

    private static File getFile(String fileName) {
        return new File(MongoConfigTest.class.getClassLoader().getResource(fileName).getFile());
    }

    @Test
    public void testMap() throws IOException {
        final Map<String, Object> map = TestHelper.createMap(true);
        final MongoConfig cfg = MongoConfig.load(map);

        assertEquals(cfg.getMongoUri(), TestHelper.URI);
        assertEquals(cfg.getDatabase(), TestHelper.DB);
        assertEquals(cfg.getCollection(), TestHelper.COLL);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Required property not set.")
    public void testBadMap() throws IOException {
        final Map<String, Object> map = TestHelper.createMap(false);
        final MongoConfig cfg = MongoConfig.load(map);

        cfg.validate();
    }

    @Test
    public void testYaml() throws IOException {
        final File yaml = getFile("mongoSinkConfig.yaml");
        final MongoConfig cfg = MongoConfig.load(yaml.getAbsolutePath());

        assertEquals(cfg.getMongoUri(), TestHelper.URI);
        assertEquals(cfg.getDatabase(), TestHelper.DB);
        assertEquals(cfg.getCollection(), TestHelper.COLL);
    }
}
