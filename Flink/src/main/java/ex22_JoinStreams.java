import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ex22_JoinStreams {

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        if(dataStream==null){
            System.exit(1);
            return;
        }


        DataStream<Tuple3<String, String, Double>> stream1 = dataStream.map(new SumNumbers());


        DataStream<Tuple3<String, String, Double>> stream2 = dataStream.map(new MultiplyNumbers());

        DataStream<Tuple3<String, Double, Double>> joinedStream =
                stream1.join(stream2).
                where(new StreamKeySelector()).equalTo(new StreamKeySelector()).
                window(TumblingProcessingTimeWindows.of(Time.seconds(30))).
                apply(new SumProductJoinFunction());

        joinedStream.print();

        env.execute("Window Join Example");
    }




    private static class StreamKeySelector implements
            KeySelector<Tuple3<String, String, Double>, String> {
        public String getKey(Tuple3<String, String, Double> value) {
            return value.f0;
        }
    }

    private static class SumProductJoinFunction implements
            JoinFunction<Tuple3<String, String, Double>,
                    Tuple3<String, String, Double>,
                    Tuple3<String, Double, Double>> {
        public Tuple3<String, Double, Double> join(
                Tuple3<String, String, Double> first,
                Tuple3<String, String, Double> second) {
            return  Tuple3.of(first.f0, first.f2, second.f2);
        }
    }




    public static class SumNumbers implements MapFunction<String, Tuple3<String, String, Double>> {
        public Tuple3<String, String, Double> map(String input) throws Exception {

            String[] nums = input.split(" ");

            Double sum = 0.0;

            for (String num:nums){
                sum = sum+Double.parseDouble(num);
            }


            return new Tuple3<String, String, Double>(input,"Sum",sum);
        }
    }

    public static class MultiplyNumbers implements MapFunction<String, Tuple3<String, String, Double>> {
        public Tuple3<String, String, Double> map(String input) throws Exception {

            String[] nums = input.split(" ");

            Double product = 0.0;

            for (String num:nums){
                product = product*Double.parseDouble(num);
            }


            return new Tuple3<String, String, Double>(input,"Product",product);
        }
    }







}
