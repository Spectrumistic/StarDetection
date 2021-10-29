# Thesis project: Star Detection on live twitter data

## Description
This is a flink based project that utilizes kafka to simulate a real incoming stream of twitter data into flink.
Data is parsed and processed and then fed into a pipeline of logical subdivisions that flink calls windows. The result of the pipeline
is the users that contribute the most to the biggest star topologies present in the network graph. This is achieved by calculating the out degree centrality of each user.

## How to run
In order for the data to be fed into flink I use a bash script that reads a months worth of data and writes it to the kafka stream.
Path variables need to be changed to run locally on another machine. I have marked them with @pathChange.

1. Install kafka locally from https://kafka.apache.org/downloads. In the makings of this project kafka 2.8.0 was used.
1. On a new terminal run `bin/zookeeper-server-start.sh config/zookeeper.properties`
1. On another terminal run `bin/kafka-server-start.sh config/server.properties`
1. Create a topic named quickstart-events by running `bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092` on another terminal
1. Step 4 has to be performed only once.
1. Run the maven project `StreamEnv_lessStrictStarDetectionTest.java` in InteliJ
1. On a terminal run the script `resources/startStream.sh`

## How to produce jar
A maven assembly plug-in included in this project can produce a jar file by running the `install` command in the maven lifecycle.
## Additional Work done
The project also contains an algorithm that works on static graphs, utilizing the dataset library and simple execution environment, and detects strict star topologies. It is located in
`starDetectionTest.java`. In contrast, the `StreamEnv_lessStrictStarDetectionTest.java` uses a stream environment and utilizes streaming solutions. 

## Troubleshooting 
* If you are having issues related to kafka logs you can delete them without breaking anything by running `rm -rf /tmp/kafka-logs`
* If you are having issues related to the server in which kafka is listening then try changing the `listeners` in the `server.properties` config file to `localhost`

## License
Distributed under the MIT License. See [LICENSE](https://opensource.org/licenses/MIT) for more information

