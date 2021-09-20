import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.BasicConfigurator;

public class starDetectionTest {

    public static void main(String[] args) throws Exception {
//        BasicConfigurator.configure();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Reading the vertices file
//        DataSet<Tuple2<Long, String>> vertexTuples = env.readCsvFile("/resources/vertices.csv").types(Long.class, String.class);

//        System.out.println("Vertices are:");
//        vertexTuples.print();

        // Reading the edges file
//        DataSet<Tuple3<Long, Long, String>> edgeTuples = env.readCsvFile("/resources/edges.csv").types(Long.class, Long.class, String.class);

//        System.out.println("Edges are:");
//        edgeTuples.print();

        // Creating the graph form the edges and vertices we read
//        Graph<Long, String, String> graph = Graph.fromTupleDataSet(vertexTuples, edgeTuples, env);

        // Reading just edges files
        DataSet<Tuple3<Long, Long, String>> edgesTuple = env.readCsvFile("file:///F:\\Users\\ismer\\Documents\\CSD\\ptuxiaki\\twitter live stream\\src\\main\\resources\\just1000coma.csv").types(Long.class, Long.class, String.class);
        Graph<Long, NullValue, String> graph = Graph.fromTupleDataSet(edgesTuple, env);

        Long numberOfVertices = graph.numberOfVertices();
        Long numberOfEdges = graph.numberOfEdges();
        System.out.println("Number of vertices is:" + numberOfVertices);
        System.out.println("Number of edges is: " + numberOfEdges);

//        The following need to be true to have a star topology
//        1.One node (the central node) has degree V – 1.
//        2.All nodes except the central node have degree 1.
//        3.# of edges = # of Vertices – 1.


        DataSet<Tuple2<Long, LongValue>> vertexDegrees = graph.getDegrees();

//        vertexDegrees.print();
        vertexDegrees.maxBy(1).print();
        System.out.println("Highest degree among all the vertices: " );

        Long oneDeg = vertexDegrees.flatMap(new singleDeg()).count();
        System.out.println("Nodes with degree one are: " + oneDeg);

        Long maxDeg = vertexDegrees.flatMap(new highDeg(numberOfVertices)).count();
        System.out.println("Nodes with degree " + (numberOfVertices - 1L) + " are: " + maxDeg);


        boolean isStar = starDetection(maxDeg, numberOfVertices, oneDeg, numberOfEdges);

        if(isStar) {
            System.out.println("Inserted Graph is of Star topology");
        }else{
            System.out.println("Inserted Graph is NOT of Star topology");
        }

        env.execute();

    }

    static boolean starDetection(Long highestDegCount, Long numberOfVertices, Long singleDegCount, Long numberOfEdges){
        boolean singleHighestDeg = false;
        boolean restWithDegOne = false;
        boolean edgesAndVerticesEquality = false;

        // step 1, find out if there is only one node with deg = highestDegCount
        if(highestDegCount == 1L) singleHighestDeg = true;

        // step 2, find out if all the other nodes have degree 1
        if(singleDegCount == numberOfVertices - 1L) restWithDegOne = true;

        // step 3, find out if # of edges = # of vertices - 1
        if(numberOfEdges == numberOfVertices - 1) edgesAndVerticesEquality = true;

        if(singleHighestDeg && restWithDegOne && edgesAndVerticesEquality){
            return true;
        }else{
            return false;
        }
    }

    public static class singleDeg implements FlatMapFunction<Tuple2<Long, LongValue>, LongValue> {
        @Override
        public void flatMap(Tuple2<Long, LongValue> node, Collector<LongValue> out) {

            if(node.f1.getValue() == 1L){
                out.collect(node.f1);
            }

        }
    }

    public static class highDeg implements FlatMapFunction<Tuple2<Long, LongValue>, LongValue> {
        Long maxDeg;

        public highDeg(Long V){
            this.maxDeg = V-1L;
        }

        @Override
        public void flatMap(Tuple2<Long, LongValue> node, Collector<LongValue> out) {

            if(node.f1.getValue() == this.maxDeg){
                out.collect(node.f1);
            }

        }
    }

}
