import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ex25_iterate {

    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> dataStream = env.fromElements(32,17,30,27);

        //Enable iteration
        IterativeStream<Tuple2<Integer,Integer>> iterativeStream = dataStream
                .map(new addIterCount())
                .iterate();

        DataStream<Tuple2<Integer,Integer>> checkMultiple4Stream = iterativeStream.map(new checkMultiple4());
        DataStream<Tuple2<Integer,Integer>> feedback = checkMultiple4Stream.filter(new FilterFunction<Tuple2<Integer, Integer>>() {

            public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
                return value.f0%4!=0;
            }
        });
        iterativeStream.closeWith(feedback);
        DataStream<Tuple2<Integer, Integer>> outputStream = checkMultiple4Stream.filter(new FilterFunction<Tuple2<Integer, Integer>>() {

            public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
                return value.f0%4==0;
            }
        });

        outputStream.print();

        env.execute("Iterate Example");

    }


    private static class addIterCount implements MapFunction<Integer, Tuple2<Integer,Integer>> {

        public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
            return Tuple2.of(integer,0);
        }
    }

    private static class checkMultiple4 implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> input) throws Exception {

            Tuple2<Integer, Integer> output;
            if (input.f0%4==0){
                output = input;
            }
            else{
                output = Tuple2.of(input.f0-1,input.f1+1);
            }
            return output;
        }
    }
}
