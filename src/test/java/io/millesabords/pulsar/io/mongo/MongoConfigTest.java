package io.millesabords.pulsar.io.mongo;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.testng.Assert.assertEquals;

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
        assertEquals(cfg.getBatchSize(), TestHelper.BATCH_SIZE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Required property not set.")
    public void testBadMap() throws IOException {
        final Map<String, Object> map = TestHelper.createMap(false);
        final MongoConfig cfg = MongoConfig.load(map);

        cfg.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "batchSize must be a positive integer.")
    public void testBadBatchSize() throws IOException {
        final Map<String, Object> map = TestHelper.createMap(true);
        map.put("batchSize", 0);
        final MongoConfig cfg = MongoConfig.load(map);

        cfg.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "batchTimeMs must be a positive long.")
    public void testBadBatchTime() throws IOException {
        final Map<String, Object> map = TestHelper.createMap(true);
        map.put("batchTimeMs", 0);
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
        assertEquals(cfg.getBatchSize(), TestHelper.BATCH_SIZE);
        assertEquals(cfg.getBatchTimeMs(), TestHelper.BATCH_TIME);
    }
}