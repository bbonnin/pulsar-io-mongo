#!/bin/bash

PULSAR_HOME=../apache-pulsar-2.1.1-incubating
HERE="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$PULSAR_HOME/bin/pulsar-admin sink create \
        --tenant public \
        --namespace default \
        --name mongo-test-sink \
        --sink-type mongo \
        --sinkConfigFile $HERE/example-mongo-sink.yaml \
        --inputs test_mongo
