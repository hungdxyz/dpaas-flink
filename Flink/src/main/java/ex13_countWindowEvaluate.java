import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import javax.swing.*;
import javax.swing.plaf.basic.BasicComboBoxUI;
import java.util.HashMap;
import java.util.Map;

// KeyedStreams
public class ex13_countWindowEvaluate {

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

        WindowedStream<courseCount,Tuple,GlobalWindow> windowedStream = dataStream
                .map(new parseRow())
                .keyBy("staticKey")
                .countWindow(5);


        DataStream<Map<String,Integer>> outStream = windowedStream.apply(new collectUS());

        outStream.print();

        env.execute("Count Window");
    }

    public static class parseRow implements MapFunction<String, courseCount> {

        public courseCount map(String input) throws Exception {

            try {
                String[] rowData = input.split(",");

                return new courseCount(rowData[0].trim(),
                        rowData[1].trim(),
                        1);
            } catch (Exception ex) {
                System.out.println(ex);
            }

            return null;
        }


    }

    //    The class must be public
    //    It must have a public constructor without arguments
    //    All fields either have to be public or there must be getters and
    //    setters for all non-public fields. If the field name is foo the
    //    getter and setters must be called getFoo() and setFoo().
    public static class courseCount {
        public Integer staticKey = 1;
        public String course;
        public String country;
        public Integer count;

        public courseCount() {
        }

        public courseCount(String course, String country, Integer count) {
            this.course = course;
            this.count = count;
            this.country= country;
        }

        public String toString() {
            return course + ": " + count;
        }
    }

    public static class collectUS implements WindowFunction<courseCount,Map<String,Integer>,
            Tuple,GlobalWindow>{


        public void apply(Tuple tuple,
                          GlobalWindow globalWindow,
                          Iterable<courseCount> iterable,
                          Collector<Map<String,Integer>> collector) throws Exception {


            Map<String,Integer> output = new HashMap<String, Integer>();

            for (courseCount signUp:iterable){

                if(signUp.country.equals("US")){

                    if (!output.containsKey(signUp.course)){
                        output.put(signUp.course,1);
                    }

                    else {
                        output.put(signUp.course, output.get(signUp.course) + 1);
                    }

                }
            }

            collector.collect(output);

        }
    }



}
