import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

// use socket + signups.txt

public class ex10_windows {

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

//       DataStream<Tuple2<String, Integer>> outStream = dataStream
//               .map(new parseRow())
//               .keyBy(0)
//               .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//               .sum(1);

        DataStream<Tuple2<String, Integer>> outStream = dataStream
                .map(new parseRow())
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .sum(1);

        outStream.print();

        env.execute("Tumbling and Sliding Window");
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
