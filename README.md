# Thesis project: Star Detection on live twitter data
**Author:** Stefanos Pleros,
**Supervisor:** Polyvios Pratikakis
---

## Description
This is a flink based project that utilizes kafka to simulate a real incoming stream of twitter data into flink.
Data is parsed and processed and then fed into a pipeline of logical subdivisions that flink calls windows. The result of the pipeline
is the users that contribute the most to the biggest star topologies present in the network graph. This is achieved by calculating the out degree centrality of each user.

## How to run
In order for the data to be fed into flink I use a bash script that reads a months worth of data and writes it to the kafka stream.
@todo: script to be added
@todo: add parsing script

1. Install kafka locally from https://kafka.apache.org/downloads. In the makings of this project kafka 2.8.0 was used.
1. On a new terminal run `bin/zookeeper-server-start.sh config/zookeeper.properties`
1. On another terminal run `bin/kafka-server-start.sh config/server.properties`
1. Create a topic named quickstart-events by running `bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092` on another terminal
1. Run the maven project
1. On a terminal run the script (that is to be added in the repo)


## Troubleshooting 
* If you are having issues related to kafka logs you can delete them without breaking anything by running `rm -rf /tmp/kafka-logs`
* If you are having issues related to the server in which kafka is listening then try changing the `listeners` in the `server.properties` config file to `localhost`