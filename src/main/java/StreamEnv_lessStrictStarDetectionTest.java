import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.graph.*;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.*;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

//import java.io.IOException;

public class StreamEnv_lessStrictStarDetectionTest {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // cd to /mnt/f/Users/ismer/Documents/CSD/ptuxiaki/kafka_2.13-2.8.0
        // CHANGED THE LISTENERS in the server.properties config file to localhost
        // then I just followed the tutorial from:
        // https://kafka.apache.org/quickstart
        // Step by step:
        // 1. bin/zookeeper-server-start.sh config/zookeeper.properties
        // 2. bin/kafka-server-start.sh config/server.properties
        // 3. ~~~ bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
        // 4. ~~~ bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("quickstart-events")
                .setGroupId("test-consumer-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // If OffsetsInitializer.latest() then we start reading events that are being streamed currently
        //  if earliest() then we start reading events from the start of the kafka session
//
        DataStream<String> graphData = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        DataStream<String> graphData  = env.readTextFile("file:///F:\\Users\\ismer\\Documents\\CSD\\ptuxiaki\\twitter live stream\\src\\main\\resources\\just1000coma.csv");
        // To feed input an file into the stream use ``cat input.txt | nc -l -p 9999``
//        DataStream<String> graphData = env.socketTextStream("localhost", 9999);

        // Creating the graph with just the edges files


//        The following need to be true to have a star topology
//        1.One node (the central node) has degree V – 1.
//        2.All nodes except the central node have degree 1.
//        3.# of edges = # of Vertices – 1.


//        DataStream<String> graphWithWatermarks = graphData.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());

        // This calculates only out degree centrality without accounting for edge weights and only for 1 file
//        DataStream<Tuple2<Long, Integer>> result = graphData.rebalance().flatMap(new Star()).keyBy(value -> value.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(1);

        // This aims to do the above but on multiple files with each file being marked as a seperate window
//        DataStream<Tuple2<Long, Integer>> result = graphData.rebalance().flatMap(new Star()).keyBy(value -> value.f0).window(GlobalWindows.create()).trigger(new FileTrigger()).sum(1);

        // This aims to do the above but the end of a window is marked by a pause in receiving data for a specific amount of time
        DataStream<Tuple2<Long, Integer>> result = graphData.rebalance().flatMap(new Star()).keyBy(value -> value.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))).sum(1);

//        result.print();

//        DataStream<Tuple2<Long, Integer>> finalResult = graphData.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5))).process(new ProcessAllWindowFunction<String, Tuple2<Long, Integer>, TimeWindow>() {
//            @Override
//            public void process(Context context, Iterable<String> iterable, Collector<Tuple2<Long, Integer>> collector) throws Exception {
//                System.out.println("processed file");
//
//                for (String s : iterable) {
//                    System.out.println("******* " + s );
//                }
//            }
//        });

        // window works by separating the keyed streams to parallel tasks, maybe that's why it wasn't working as expected b4.
        DataStream<Tuple2<Long, Integer>> finalResult = result.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(25))).process(new ProcessAllWindowFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Tuple2<Long, Integer>> iterable, Collector<Tuple2<Long, Integer>> collector) throws Exception {

              for (Tuple2<Long, Integer> longIntegerTuple2 : iterable) {
                  // instead we can collect other results here
                    collector.collect(longIntegerTuple2);
                    // here we could process additional data
              }
            }
        }).keyBy(v -> v.f0).sum(1)
                .filter(new FilterFunction<Tuple2<Long, Integer>>() {
            @Override
            public boolean filter(Tuple2<Long, Integer> longIntegerTuple2) throws Exception {
                if(longIntegerTuple2.f1 < 1000){
                    return false;
                }
                return true;
            }
        }).flatMap(new ExistingStar());

//        DataStreamSink sink =  finalResult.print();

//        DataStream<Tuple2<Long, Integer>> globalResult = result.keyBy(v -> v.f0).window(GlobalWindows.create()).sum(1);
//
//        globalResult.print();

        String outputPath = "F:\\Users\\ismer\\Documents\\CSD\\ptuxiaki\\dataset\\twitterdata\\results";

//        final StreamingFileSink<Tuple2<Long, Integer>> actualSink = StreamingFileSink.forRowFormat(new Path(outputPath),
//                new SimpleStringEncoder<Tuple2<Long, Integer>>("UTF-8")).withRollingPolicy( DefaultRollingPolicy.builder()
//                .withRolloverInterval(TimeUnit.MINUTES.toMillis(20))
//                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//                .withMaxPartSize(1024 * 1024 * 1024)
//                .build())
//                .build();


//        finalResult.addSink(actualSink);

        // ok this works as a data iterator, but is it the proper mapping method? Also we need to process the hashmap once
        //it has finished adding elements to it.

        env.execute("Star Detection");

    }

    public static class FileTrigger extends Trigger<Tuple2<Long, Integer>, Window> {

        @Override
        public TriggerResult onElement(Tuple2<Long, Integer> element, long timestamp, Window window, TriggerContext ctx) throws Exception {

            // identifier that splits the input in different files (windows)
            if(element.f0 == -10000){
                System.out.println("We found at least 2 files");
                return TriggerResult.FIRE_AND_PURGE;
            }else{
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(Window window, OnMergeContext ctx) throws Exception {
            ctx.registerProcessingTimeTimer(window.maxTimestamp());
        }

        @Override
        public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(Window window, TriggerContext ctx) throws Exception {
        }

    }



    public class FlatMapFunctionException extends Exception {
        public FlatMapFunctionException(String errorMessage) {
            super("[Error in FlatMap]:" + errorMessage);
        }
    }


    // should this calculate total degree or just outdegree? (right now its only out degree and if it is enough for now)
    public static class Star implements FlatMapFunction<String, Tuple2<Long, Integer>> {

        public static HashMap<Long, ArrayList<Long>> Relations = new HashMap<Long, ArrayList<Long>>();

//        public Star(HashMap<Long, ArrayList<Long>> rel) {
//            this.Relations = rel;
//        }

        @Override
        public void flatMap(String value, Collector<Tuple2<Long, Integer>> out) throws FlatMapFunctionException {
            String[] parts = value.split(",");
            if(parts.length == 1) {
                // missing edge info
                System.out.println("Detected invalid edge ignoring... ");
//                out.collect(new Tuple2<Long, Integer>(Long.parseLong(parts[0].trim()), 1));
                return;
            }
//            System.out.println("User 1: " + parts[0]);
//            System.out.println("User 2: " + parts[1]);
            // Should handle 3rd part eventually
            Vertex<Long, String> from = new Vertex<>(Long.parseLong(parts[0].trim()), "UserFrom");
            Vertex<Long, String> to = new Vertex<>(Long.parseLong(parts[1].trim()), "UserTo");

            // Retweeted should eventually be replaced with the 3rd part value;
//            graph.addEdge(from, to, "Retweeted");

            synchronized (Relations) {


                if (from.f0 == to.f0) {
                    System.out.println("Detected self edge ignoring... ");
                    // we should not add self edges
                    return;
                }

                if (from.f0 != null && to.f0 != null) {
                    // no "collisions" yet
                    if (Relations.get(from.f0) == null) {
                        ArrayList<Long> newArray = new ArrayList<Long>();
                        newArray.add(to.f0);
                        Relations.put(from.f0, newArray);
                    } else {
                        // starting to have "collisions"
                        ArrayList<Long> existingArray = Relations.get(from.f0);
//                    System.out.println("In flatmap " + existingArray.size());
                        // sometimes this throws array index out of bounds exception ??? and some times null pointer????
                        existingArray.add(to.f0);
                    }
                }
//                System.out.println("Kafka and Flink says: " + value + " " + (Relations.get(from.f0) != null ? Relations.get(from.f0).size() : "-"));

            }
            out.collect(new Tuple2<Long, Integer>(from.f0, 1));
        }

    }

    public static class ExistingStar implements FlatMapFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>> {


        private Long countDuplicates(ArrayList<Long> clique) {

            Set<Long> uniqueUsers = new HashSet<Long>(clique);
            Long duplicates = 0L;

            for (Long user : uniqueUsers) {

//                System.out.println("User " + user + " duplicates " +  Collections.frequency(clique, user));
                int freq = Collections.frequency(clique, user);

                if(freq > 2L){
                    duplicates += freq;
                }

            }

            return duplicates;
        }

        // on 12829 prints all apart from 882 were 'persistent' with freq > 2 and pers > 50%
        // on 12829 prints all apart from 2036 were 'persistent' with freq > 2 and pers > 60%
        // on 12829 prints all apart from 11354 were 'persistent' with freq > 2 and pers > 90%
        private Boolean isPersistent(Long count, Integer total) {

            if((count * 100/total) > 90) {
                return true;
            }

            return  false;
        }

        @Override
        public void flatMap(Tuple2<Long, Integer> value, Collector<Tuple2<Long, Integer>> out) throws FlatMapFunctionException {
            Integer size = 0;

            if( Star.Relations.get(value.f0) != null) {
                ArrayList<Long> tmp = Star.Relations.get(value.f0);
                size = tmp.size();

//                System.out.println("This is " + value.f0 + " and the size is " + size + " should match with out degree " + value.f1);
//                System.out.println(tmp.toString());

                // in theory, every entry time we check the Relations data structure here, we get a list of the filtered
                // users a user has retweeted from/to up to this time.
                // Meaning that each time we have information for the current window AND for each consecutive one
                // information about all the past windows is kept including the present one

                // How should we define a clique that forms as one that's persistent?
                // * Could count how many times per month a user appears in the star topology of another user and if enough
                //   users satisfy a certain threshold, for example more than 50% of the stars topology consists of the
                //   same users (duplicates or another threshold) it can be marked as suspicious or as an interesting
                //   clique to study.
                //   (for a period of 20 days or more) -> This is interesting but requires extra parameters to be added
                //
                //   This seems promising but has abstract threshold variable/s which isn't necessarily bad, it can be something
                //   configurable, more of a statistic observation rather than proof of something.

                // How to implement the above:
                // Relations can provide us with both the final out degree of each user star and the user id of their
                // neighbors. The above formula would be something like (#ofDuplicates * 100/#totalUsersInTheStar) > 50%
                //
                // * Need a function to count how many times each user appears in a given star

                ArrayList<Long> clique = Star.Relations.get(value.f0);
                Long duplicates = countDuplicates(clique);

                Boolean isPersistent = isPersistent(duplicates, clique.size());

                System.out.println("User " + value.f0 + " duplicates " + duplicates + " persistent " + isPersistent + " size " +  clique.size());
                if(isPersistent){
                    out.collect(new Tuple2<Long, Integer>(value.f0, value.f1));
                }
            }

        }

    }

}

