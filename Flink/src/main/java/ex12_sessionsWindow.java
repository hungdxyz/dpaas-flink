import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

// KeyedStreams, Reduce, multi-value tuples
public class ex12_sessionsWindow {

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        if (dataStream == null) {
            System.exit(1);
            return;
        }

       DataStream<Tuple2<String, Integer>> outStream = dataStream
               .map(new parseRow())
               .keyBy(0)
               .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
               .sum(1);

        outStream.print();

        env.execute("Course Count");
    }


    public static class parseRow implements MapFunction<String, Tuple2<String,Integer>> {

        public Tuple2<String,Integer> map(String input) throws Exception {

            try {
                String[] rowData = input.split(",");

                return new Tuple2<String,Integer>(
                        rowData[1].trim(),
                        1);
            } catch (Exception ex) {
                System.out.println(ex);
            }

            return null;
        }
    }



}
