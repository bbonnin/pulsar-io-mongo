package io.millesabords.pulsar.io.mongo;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.bulk.BulkWriteError;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.bson.BsonDocument;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@PrepareForTest(MongoClients.class)
@PowerMockIgnore({"org.apache.logging.log4j.*"})
public class MongoSinkTest {

    @Mock
    private Record<byte[]> mockRecord;

    @Mock
    private SinkContext mockSinkContext;

    @Mock
    private MongoClient mockMongoClient;

    @Mock
    private MongoDatabase mockMongoDb;

    @Mock
    private MongoCollection mockMongoColl;

    private MongoSink sink;

    private Map<String, Object> map;


    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @BeforeMethod
    public void setUp() {
        sink = new MongoSink();
        map = TestHelper.createMap(true);

        mockRecord = mock(Record.class);
        mockSinkContext = mock(SinkContext.class);
        mockMongoClient = mock(MongoClient.class);
        mockMongoDb = mock(MongoDatabase.class);
        mockMongoColl = mock(MongoCollection.class);

        PowerMockito.mockStatic(MongoClients.class);

        when(MongoClients.create(anyString())).thenReturn(mockMongoClient);
        when(mockMongoClient.getDatabase(anyString())).thenReturn(mockMongoDb);
        when(mockMongoDb.getCollection(anyString())).thenReturn(mockMongoColl);
    }

    private void initContext(boolean throwBulkError) {
        when(mockRecord.getValue()).thenReturn("{\"hello\":\"pulsar\"}".getBytes());

        doAnswer((invocation) -> {
            SingleResultCallback cb = invocation.getArgumentAt(1, SingleResultCallback.class);
            MongoBulkWriteException exc = null;

            if (throwBulkError) {
                List<BulkWriteError > writeErrors = Arrays.asList(
                        new BulkWriteError(0, "error", new BsonDocument(), 1));
                exc = new MongoBulkWriteException(null, writeErrors, null, null);
            }

            cb.onResult(null, exc);
            return null;
        }).when(mockMongoColl).insertMany(anyObject(), anyObject());
    }

    private void initFailContext(String msg) {
        when(mockRecord.getValue()).thenReturn(msg.getBytes());

        doAnswer((invocation) -> {
            SingleResultCallback cb = invocation.getArgumentAt(1, SingleResultCallback.class);
            cb.onResult(null, new Exception("Oops"));
            return null;
        }).when(mockMongoColl).insertMany(anyObject(), anyObject());
    }

    @AfterMethod
    public void tearDown() throws Exception {
        sink.close();
        verify(mockMongoClient, times(1)).close();
    }

    @Test
    public void testOpen() throws Exception {
        sink.open(map, mockSinkContext);
    }

    @Test
    public void testWriteNullMessage() throws Exception {
        when(mockRecord.getValue()).thenReturn("".getBytes());

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(1)).fail();
    }

    @Test
    public void testWriteGoodMessage() throws Exception {
        initContext(false);

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(1)).ack();
    }

    @Test
    public void testWriteMultipleMessages() throws Exception {
        initContext(true);

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);
        sink.write(mockRecord);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(2)).ack();
        verify(mockRecord, times(1)).fail();
    }

    @Test
    public void testWriteWithError() throws Exception {
        initFailContext("{\"hello\":\"pulsar\"}");

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(1)).fail();
    }

    @Test
    public void testWriteBadMessage() throws Exception {
        initFailContext("Oops");

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(1)).fail();
    }
}