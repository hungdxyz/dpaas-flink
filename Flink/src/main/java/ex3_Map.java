import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// use socket
// Map, and Chaining
public class ex3_Map {

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

        DataStream<String> outStream = dataStream
                .filter(new Filter())
                .map(new CleanString());

        outStream.print();


        env.execute("Clean up strings");
    }

    public static class Filter implements FilterFunction<String> {

        public boolean filter(String input) throws Exception {
            try {
                Double.parseDouble(input.trim());
                return false;
            } catch (Exception ex) {
            }

            return input.length()>3;
        }

    }

    public static class CleanString implements MapFunction<String, String> {
        public String map(String input) throws Exception {

            return input.trim().toLowerCase();
        }
    }

}
