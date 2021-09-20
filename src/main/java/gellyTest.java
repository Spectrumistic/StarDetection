import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.*;
import org.apache.flink.types.NullValue;

public class gellyTest {

    public static void main(String[] args) throws Exception {
        // ID and value
        Vertex<Long, String> v = new Vertex<Long, String>(1L, "foo");

        System.out.println(v);

        // ID(from), target ID(to), optional value(weight)
        Edge<Long, Double> e = new Edge<Long, Double>(1L, 2L, 0.5);

        System.out.println(e);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Vertex<Long, String> v2 = new Vertex<Long, String>(2L, "bar");
        Vertex<Long, String> v3 = new Vertex<Long, String>(3L, "foobar");
        // Creating a dataset of vertices
        DataSet<Vertex<Long, String>> vertices = env.fromElements(v, v2, v3);

        Edge<Long, Double> e2 = new Edge<Long, Double>(2L, 3L, 0.2);
        Edge<Long, Double> e3 = new Edge<Long, Double>(3L, 1L, 0.9);

        // Creating a dataset of edges
        DataSet<Edge<Long, Double>> edges = env.fromElements(e, e2, e3);
        System.out.println("edges are ;");
        edges.print();
        // Creating a graph with an optional DataSet of vertices(?)
        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, edges, env);


        DataSet<Tuple2<String, String>> tupleEdges = env.fromElements(new Tuple2<String, String>("1", "2"),
        new Tuple2<String, String>("2", "1"));

        // Creating a graph with just the edges
        Graph<String, NullValue, NullValue> graphWithoutVertices = Graph.fromTuple2DataSet(tupleEdges, env);

        // Question on initialization with map function: arg(value) is passed automagically from edgeList(?)

        System.out.println(graph.numberOfVertices());


        Graph<Long, String, Double> subGraph = graph.subgraph(
                new FilterFunction<Vertex<Long, String>>() {
                    @Override
                    public boolean filter(Vertex<Long, String> longStringVertex) throws Exception {
                        return (longStringVertex.getId() > 1);
                    }
                },
                new FilterFunction<Edge<Long, Double>>() {
                    @Override
                    public boolean filter(Edge<Long, Double> longDoubleEdge) throws Exception {
                        return false;
                    }
                }
        );

        System.out.println(subGraph.numberOfVertices());


        DataSet<Tuple2<Long, Double>> minWeights = graph.reduceOnEdges(new SelectMinWeight(), EdgeDirection.OUT);

        minWeights.print();
    }

    static final class SelectMinWeight implements ReduceEdgesFunction<Double> {

        @Override
        public Double reduceEdges(Double firstEdgeValue, Double secondEdgeValue) {
            return Math.min(firstEdgeValue, secondEdgeValue);
        }
    }

}

