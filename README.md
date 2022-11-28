# kafka-play

Trying out things in Kafka. The Kafka instance has the sample data connector enabled, which generates automatic entries with fake stock price updates. The topic uses the protobuf schema registry.

## Kafka.Test.Consumer.Api
Web App based consumer.

Worker - Listening to stocks topic using a thread for consumption and a thread for production
LatencyWorker - Listening to separate latency topic to measure latency on produce/consume side together (from the Producer project)

Maximum throughput measured: 27k events/sec
Minimum latency measured: ~35ms (Melbourne to Sydney is min 15ms)
Maximum latency measured: ~120ms

## Kafka.Test.Consumer.Func
Isolated Function App consumer using Kafka Triggers.

Maximum throughput measured: ~2k events/sec
Minimum latency measured: ~140ms
Maximum latency measured ~4400ms

## Kafka.Test.Consumer.Worker.Confluent
Worker service, similar to the webapp without the web part.

## Kafka.Test.Overview
NOT COMPLETE - Supposed to be a dashboard which displays the statistics, but requires the application to be running and a datastore for the statistics. Maybe just use other tooling such as Prometheus?

## Kafka.Test.Producer.LatencyTest
Producer for the Latency test

## Kafka.Test.Schemas
Store the proto files from the schema registry, and shared library for Kafka configuration and materialized views.
