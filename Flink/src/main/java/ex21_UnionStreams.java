import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ex21_UnionStreams {

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

        if (stream1 == null || stream2 == null) {
            System.exit(1);
            return;
        }

        DataStream<Tuple3<String, String, Double>> unionStream = stream1.union(stream2);
        unionStream.print();

        env.execute("Union");
    }

    public static class SumNumbers implements MapFunction<String, Tuple3<String, String, Double>> {
        public Tuple3<String, String, Double> map(String input) throws Exception {

            String[] nums = input.split(" ");

            Double sum = 0.0;

            for (String num:nums){
                sum = sum+Double.parseDouble(num);
            }


            return Tuple3.of(input,"Sum",sum);
        }
    }

    public static class MultiplyNumbers implements MapFunction<String, Tuple3<String, String, Double>> {
        public Tuple3<String, String, Double> map(String input) throws Exception {

            String[] nums = input.split(" ");

            Double product = 0.0;

            for (String num:nums){
                product = product*Double.parseDouble(num);
            }


            return  Tuple3.of(input, "Product", product);
        }
    }






}
