## Kafka

This directory contains configuration for running a local Kafka instance. This can be used to stream events into Spark and is more production ready than the current technique of using a TCP socket. 

There is commented code in `streaming.py` for connecting to Kafka. Additionally, `darwin-listener.py` would need to be updated to write the incoming events to a Kafka topic. (TODO)