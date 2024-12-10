import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
// use courses.txt


// FlatMap
public class ex8_reduce {

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = 
            StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env,params);
        if(dataStream==null){
            System.exit(1);
            return;
        }

        DataStream<Tuple2<String,Double>> outStream = dataStream.
                map(new parseRow())
                .keyBy(0)
                .reduce(new SumAndCount())
                .map(new Average());

        outStream.print();

        env.execute("Find Average course length");
    }

    public static class parseRow implements MapFunction<String, Tuple3<String,Double,Integer>> {

        public Tuple3<String, Double, Integer> map(String input) throws Exception {

            try {
                String[] rowData = input.split(",");

                return new Tuple3<String, Double, Integer>(
                        rowData[2].trim(),
                        Double.parseDouble(rowData[1]),
                        1);
            } catch (Exception ex) {
                System.out.println(ex);
            }

            return null;
        }


    }

        public static class SumAndCount implements
                ReduceFunction<Tuple3<String, Double, Integer>> {

            public Tuple3<String, Double, Integer> reduce(
                    Tuple3<String, Double, Integer> cumulative,
                    Tuple3<String, Double, Integer> input) {


                return new Tuple3<String, Double, Integer>(
                        input.f0,
                        cumulative.f1 + input.f1,
                        cumulative.f2 + 1);
            }
        }

        public static class Average implements
                MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>> {

            public Tuple2<String, Double> map(Tuple3<String, Double, Integer> input) {
                return new Tuple2<String, Double>(
                        input.f0,
                        input.f1 / input.f2);
            }
        }


}










