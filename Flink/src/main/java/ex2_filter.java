import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// use socket
public class ex2_filter {

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

        DataStream<String> outStream = dataStream.filter(new Filter());

        outStream.print();

        env.execute("ex1_filter");
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

}
