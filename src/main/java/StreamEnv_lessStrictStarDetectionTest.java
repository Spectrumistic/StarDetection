import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.graph.*;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.*;
import java.io.*;
import com.opencsv.CSVWriter;
import java.lang.Long;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
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

//        DataStream<String> graphWithWatermarks = graphData.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());

        DataStream<AdjacencyList> result = graphData.rebalance().flatMap(new Star()).keyBy(adjacencyList -> adjacencyList.getUser())
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))).reduce(new ReduceFunction<AdjacencyList>() {

                    // convert the edge stream into a
                    @Override
                    public AdjacencyList reduce(AdjacencyList t1, AdjacencyList t2) throws Exception {

                        ArrayList<ArrayList<Long>> dayEdges = new ArrayList<>();
                        ArrayList<Long> edges = new ArrayList<>();

                        if(t1.getEdges().size() >= 1 && t2.getEdges().size() >= 1) {
                            edges = t1.getEdges().get(0);
                            edges.addAll(t2.getEdges().get(0));

                            // probably should be removed, never gonna end in here
                        }else if(t1.getEdges().size() == 0) {
                            edges.addAll(t2.getEdges().get(0));
                        }else if(t2.getEdges().size() == 0){
                            edges.addAll(t1.getEdges().get(0));
                        }

                        dayEdges.add(edges);

                        AdjacencyList reducedT1 = new AdjacencyList(t1.getUser(), t1.getWeight() + t2.getWeight(), dayEdges);

                        return reducedT1;
                    }
                });
//                .sum(1);

        // process is slow to begin with as well
        DataStream<ArrayList<Tuple2<Long, Tuple2<Long, Long>>>> finalRes = result.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(25))).apply(new AllWindowFunction<AdjacencyList, AdjacencyList, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<AdjacencyList> iterable, Collector<AdjacencyList> collector) throws Exception {
                for(AdjacencyList it: iterable) {
                    collector.collect(it);
                }
            }
        }).keyBy(v -> v.getUser()).
        reduce(new ReduceFunction<AdjacencyList>() {

                    // convert the edge stream into a
                    @Override
                    public AdjacencyList reduce(AdjacencyList t1, AdjacencyList t2) throws Exception {

                        ArrayList<ArrayList<Long>> dayEdges = new ArrayList<>();
//                        ArrayList<Long> edges = new ArrayList<>();

                        if(t1.getEdges().size() >= 1 && t2.getEdges().size() >= 1) {
                            dayEdges = t1.getEdges();
                            dayEdges.addAll(t2.getEdges());
                        }else {
                            // idk
                        }

//                        dayEdges.add(edges);

                        AdjacencyList reducedT1 = new AdjacencyList(t1.getUser(), t1.getWeight() + t2.getWeight(), dayEdges);

                        return reducedT1;
                    }
                }).filter(new FilterFunction<AdjacencyList>() {
                    @Override
                    public boolean filter(AdjacencyList adjacencyList) throws Exception {
                        if(adjacencyList.getOutDegree() > 1000L) {
                            return true;
                        }else {
                            return false;
                        }
                    }
                }).flatMap(new FlatMapFunction<AdjacencyList, ArrayList<Tuple2<Long, Tuple2<Long, Long>>>>() {

            @Override
            public void flatMap(AdjacencyList adjacencyList, Collector<ArrayList<Tuple2<Long, Tuple2<Long, Long>>>> collector) throws Exception {
                collector.collect(adjacencyList.softIntersect(adjacencyList.getEdges().toArray(new ArrayList[0])));

            }
        }).setParallelism(5);

        // perform the session window logic here but with sliding windows
//        result.keyBy(v -> v.getUser()).window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));

//        result.print();

//        DataStreamSink sink =  finalResult.print();

//        DataStream<Tuple2<Long, Integer>> globalResult = result.keyBy(v -> v.f0).window(GlobalWindows.create()).sum(1);
//
//        globalResult.print();


        // @pathCahnge to the path you want the results of persistent stars to be saved.
        String outputPath = "F:\\Users\\ismer\\Documents\\CSD\\ptuxiaki\\dataset\\twitterdata\\results-t20-total\\";
        outputPath.replace("\\", "/");
        finalRes.flatMap(new FlatMapFunction<ArrayList<Tuple2<Long, Tuple2<Long, Long>>>, Tuple2<Long, Long>>() {

            @Override
            public void flatMap(ArrayList<Tuple2<Long, Tuple2<Long, Long>>> persStar, Collector<Tuple2<Long, Long>> collector) throws Exception {

                try {
                    String fileName;
                    if(persStar.size() != 0) {
                        fileName = outputPath + persStar.get(0).f0 + ".csv";
//                    }
//                    else{
//                        Random ran = new Random();
//                        fileName = outputPath + "na" + ran.nextLong() + ".csv";
//                    }
                        File file = new File(fileName);
                        FileWriter outputfile = new FileWriter(file);
                        CSVWriter writer = new CSVWriter(outputfile);
                        String[] header = { "Central_User", "Retweet_To", "Number_Of_Days" };
                        writer.writeNext(header);
                        for (Tuple2<Long, Tuple2<Long, Long>> it : persStar) {
                            // it.f0 users star, it.f1.f0 edge to user in adjacency list, it.f1.f1 # of days present
                            String[] data = {it.f0.toString(), it.f1.f0.toString(), it.f1.f1.toString()};
                            writer.writeNext(data);
                        }
                        System.out.println("Closed file with path " + outputPath + fileName + " - entries " + persStar.size());
                        writer.close();
                    }

                }catch(Exception e){
                    System.out.println("Could not process user ");
                    e.printStackTrace();
                }
            }
        }).setParallelism(5);



//        final StreamingFileSink<Tuple2<Long, Integer>> actualSink = StreamingFileSink.forRowFormat(new Path(outputPath),
//                new SimpleStringEncoder<Tuple2<Long, Integer>>("UTF-8")).withRollingPolicy( DefaultRollingPolicy.builder()
//                .withRolloverInterval(TimeUnit.MINUTES.toMillis(20))
//                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//                .withMaxPartSize(1024 * 1024 * 1024)
//                .build())
//                .build();


//        finalResult.addSink(actualSink);

        env.execute("Star Detection");

    }

    // does not work as intended
    public static class OnDayTrigger extends Trigger<AdjacencyList, Window> {

        private int count = 0;
        @Override
        public TriggerResult onElement(AdjacencyList element, long timestamp, Window window, TriggerContext ctx) throws Exception {

            count += 1;
            // identifier that splits the input in different files (windows)
            if(count == 5){
                System.out.println("Days: " + count);
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
            return TriggerResult.CONTINUE;
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

    public static class AdjacencyList {
        private Long user;
        private Integer weight;
        private ArrayList<ArrayList<Long>> edges;
        // day information here?


        @Override
        public String toString() {
            return "AdjacencyList{" +
                    "user=" + this.user +
                    ", weight=" + this.weight +
                    ", edgesSize=" + this.edges.size() +
                    '}';
        }

        public AdjacencyList(Long user, Integer weight, ArrayList<ArrayList<Long>> edges){
            this.user = user;
            this.weight = weight;
            this.edges = edges;
        }

        public Long getOutDegree(){
            Long count = 0L;
            for(ArrayList<Long> edgeList : edges){
                count += edgeList.size();
            }

            return count;
        }

        public void hardIntersect(ArrayList<Long> ...edges){

            for(ArrayList<Long> edge : edges){
                this.edges.retainAll(edge);
            }

        }

        // edges here represent adjacency lists of the same user
        // ara O(n^3) logika
        public ArrayList<Tuple2<Long, Tuple2<Long, Long>>> softIntersect(ArrayList<Long> ...edgesLists) {

            HashMap<Long, Long> appearanceOverTime = new HashMap<>();
            ArrayList<Tuple2<Long, Tuple2<Long, Long>>> consistentStar = new ArrayList<>();
            AdjacencyList toRet = new AdjacencyList(this.user, this.weight, null);
            // O(n)
            for(ArrayList<Long> edgeList : edgesLists) {

                // all the unique adjacent users on a given day.
                // should probably not be used as a starting point since we want the

                // **C** O(n)
                Set<Long> uniqueUsersSet = new HashSet<Long>(edgeList);
//                ArrayList<Long> uniqueUsers = new ArrayList<>();
//                uniqueUsers.addAll(uniqueUsersSet);

                // **C** O(n-dups)
                for(Long user : uniqueUsersSet){

                    Long times = appearanceOverTime.get(user);
                    if(times == null || times == 0L){
                        appearanceOverTime.put(user, 1L);
                    }else{
                        Long newTimes = times + 1;
                        appearanceOverTime.put(user, newTimes);
                    }
                }
                // do we even care about duplicates in here?
                // Probably but for some other metric
//                long duplicates = 0L;
//
//                for (Long user : uniqueUsers) {
//
//                    int freq = Collections.frequency(edge, user);
//
//                    if(freq > 2L){
//                        duplicates += freq;
//                    }
//
//                }
            }

            // O(m)
//            Iterator appear = appearanceOverTime.entrySet().iterator();
//            while(appear.hasNext()){
//                Map.Entry mapElement = (Map.Entry)appear.next();
//                Long user = (Long)mapElement.getKey();
//                Long days = (Long)mapElement.getValue();
//                if(days > 20){
////                    System.out.println("User " + user + " has appeared " + days + " days in user's " + this.user + " star ");
//                    Tuple2<Long, Tuple2<Long, Long>> perUser = new Tuple2<Long, Tuple2<Long, Long>>(this.user, new Tuple2<>(user, days));
//                    consistentStar.add(perUser);
//                    // and return this? maybe add some extra info
//                }
//            }
            appearanceOverTime.forEach((user, days) -> {

                if(days > 20){
//                    System.out.println("User " + user + " has appeared " + days + " days in user's " + this.user + " star ");
                    Tuple2<Long, Tuple2<Long, Long>> perUser = new Tuple2<Long, Tuple2<Long, Long>>(this.user, new Tuple2<>(user, days));
                    consistentStar.add(perUser);
                    // and return this? maybe add some extra info
                }
            });

//            ArrayList<ArrayList<Long>> finalStar = new ArrayList<>();
//            finalStar.add(consistentStar);
//
//            toRet.setEdges(finalStar);
            return consistentStar;

        }

        public Long getUser() {
            return user;
        }

        public void setUser(Long user) {
            this.user = user;
        }

        public Integer getWeight() {
            return this.weight;
        }

        public void setWeight(Integer weight) {
            this.weight = weight;
        }

        public ArrayList<ArrayList<Long>> getEdges() {
            return this.edges;
        }

        public void setEdges(ArrayList<ArrayList<Long>> edges) {
            this.edges = edges;
        }
    }

    // should this calculate total degree or just outdegree? (right now its only out degree and if it is enough for now)
    public static class Star implements FlatMapFunction<String, AdjacencyList> {

        // Probably creating both this way is redundant, should be optimized
        // Monthly adjacency list
        public static HashMap<Long, ArrayList<Long>> Relations = new HashMap<Long, ArrayList<Long>>();
        // should daily adjacency list be here? Probably
        public static HashMap<Integer, HashMap<Long, ArrayList<Long>>> dailyAdjacencyList = new HashMap<>();

        public static int test = 0;

        // how do we know its the end of a day?

        @Override
        public void flatMap(String value, Collector<AdjacencyList> out) throws FlatMapFunctionException {
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
            Integer weight = Integer.parseInt(parts[2]);
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
                        existingArray.add(to.f0);
                    }
                }
//                System.out.println("Kafka and Flink says: " + value + " " + (Relations.get(from.f0) != null ? Relations.get(from.f0).size() : "-"));

            }

            ArrayList<ArrayList<Long>> edges = new ArrayList<>();
            ArrayList<Long> edge = new ArrayList<>();
            edge.add(to.f0);
            edges.add(edge);
            AdjacencyList newObject = new AdjacencyList(from.f0, weight, edges);
            out.collect(newObject);
        }

    }

    // this performs the first star persistent search approach
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

//                System.out.println("User " + value.f0 + " duplicates " + duplicates + " persistent " + isPersistent + " size " +  clique.size());
                if(isPersistent){
                    out.collect(new Tuple2<Long, Integer>(value.f0, value.f1));
                }
            }

        }

    }

}

