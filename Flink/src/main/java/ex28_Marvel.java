import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.generator.StarGraph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

// use socket
public class ex28_Marvel {

    public static void main(String[] args) throws Exception {



        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<Tuple3<Long,String,Integer>> vertexData = env.readCsvFile(params.get("vertices"))
                .fieldDelimiter("|")
                .ignoreInvalidLines()
                .types(Long.class, String.class, Integer.class)
                ;

        DataSet<Tuple2<Long,Tuple2<String, Integer>>> vertexTuples  = vertexData
                .map(new getVertex());


        DataSet<Tuple3<Long,Long,Integer>> edgeTuples  = env.readCsvFile(params.get("edges"))
                .fieldDelimiter(",")
                .ignoreInvalidLines()
                .types(Long.class, Long.class, Integer.class);


        Graph<Long, Tuple2<String, Integer>, Integer> graph =
                Graph.fromTupleDataSet(vertexTuples,edgeTuples,env);

        System.out.println("Number of edges: "+graph.numberOfEdges());
        System.out.println("Number of vertices: " + graph.numberOfVertices());



    }


    private static class getVertex implements MapFunction<Tuple3<Long, String, Integer>,
            Tuple2<Long, Tuple2<String, Integer>>> {
        public Tuple2<Long, Tuple2<String, Integer>> map(Tuple3<Long, String, Integer> input) throws Exception {
            return Tuple2.of(input.f0,Tuple2.of(input.f1,input.f2));
        }
    }
}
