# pulsar-io-mongo
MongoDB Sink for Apache Pulsar

## How to

* Start MongoDB
```
docker run --name some-mongo -d mongo:tag

# Connect to the image using bash
docker exec -it some-mongo bash
```

* Install the connector
```
$ cd <PULSAR_HOME_DIRECTORY>
$ mkdir connectors
$ cp <PATH_TO_MONGO_CONNECTOR>/pulsar-io-mongo-2.1.1-incubating.nar connectors
```


* Start Pulsar
```
pulsar standalone
```

* Check the connectors
```
curl -s http://localhost:8080/admin/v2/functions/connectors
```

* Configure a MongoDB sink
    * Create a config file `mongo-sink.yaml`
    ```yaml
    configs:
        mongoUri: "mongodb://localhost"
        database": "pulsar"
        collection": "messages"
    ```
    
    * Submit the MongoDB sink
    ```
    bin/pulsar-admin sink create \
        --tenant public \
        --namespace default \
        --name mongo-test-sink \
        --sink-type mongo \
        --sinkConfigFile mongo-sink.yaml \
        --inputs test_mongo
    ```
    
    * Retrieve sink info
    ``` 
    bin/pulsar-admin functions get \
        --tenant public \
        --namespace default \
        --name mongo-test-sink
    ```
    
    * Check running status
    ``` 
    bin/pulsar-admin functions getstatus \
        --tenant public \
        --namespace default \
        --name mongo-test-sink
    ```
    
    
* Produce some messages
``` 
for i in {0..9}; do bin/pulsar-client produce -m "{'hello':$i}" -n 1 test_mongo; done

```

* Delete the MongoDB Sink
```
bin/pulsar-admin sink delete \
    --tenant public \
    --namespace default \
    --name mongo-test-sink
```