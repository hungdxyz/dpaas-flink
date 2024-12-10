import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.io.Serializable;

public class ex14_windowTime {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params)
                .assignTimestampsAndWatermarks(new TimestampExtractor());

        if (dataStream == null) {
            System.exit(1);
            return;
        }

       DataStream<Tuple2<String, Integer>> outStream = dataStream
               .map(new parseRow())
               .keyBy(0)
               .window(TumblingEventTimeWindows.of(Time.seconds(10)))
               .sum(1);

//        DataStream<Tuple2<String, Integer>> wordCountStream = dataStream
//                .flatMap(new WordCountSplitter())
//                .keyBy(0)
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)))
//                .sum(1);

        outStream.print();

        env.execute("Tumbling and Sliding Window");
    }

    public static class parseRow implements MapFunction<String, Tuple2<String,Integer>> {

        public Tuple2<String,Integer> map(String input) throws Exception {

            try {
                String[] rowData = input.split(",");

                return new Tuple2<String,Integer>(
                        rowData[1],
                        1);
            } catch (Exception ex) {
                System.out.println(ex);
            }

            return null;
        }


    }

    public static class TimestampExtractor implements
            Serializable, AssignerWithPunctuatedWatermarks<String> {

        public long extractTimestamp(String s, long l) {

            return Long.parseLong(s.split(",")[2].trim());
        }

        @Nullable
        public Watermark checkAndGetNextWatermark(String s, long l) {
            return new Watermark(Long.parseLong(s.split(",")[2].trim()));
        }
    }
}
