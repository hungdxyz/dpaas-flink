import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


// use socket

public class ex7_NumberAggregations {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        if (dataStream == null) {
            System.exit(1);
            return;
        }

        DataStream<Tuple2<String, Double>> minStream = dataStream
                .map(new RowSplitter())
                .keyBy(0)
                .min(1);

        minStream.print();

        env.execute("Aggregations");
    }


    public static class RowSplitter implements
            MapFunction<String, Tuple2<String, Double>> {

        public Tuple2<String, Double> map(String row)
                throws Exception {
            try {
                String[] fields = row.split(" ");
                if (fields.length == 2) {
                    return new Tuple2<String, Double>(
                            fields[0] /* name */,
                            Double.parseDouble(fields[1]) /* running time in minutes */);
                }
            } catch (Exception ex) {
                System.out.println(ex);
            }

            return null;
        }
    }
}
