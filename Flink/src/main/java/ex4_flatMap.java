import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ex4_flatMap {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = 
            StreamExecutionEnvironment.getExecutionEnvironment();


        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env,params);
        if(dataStream==null){
            System.exit(1);
            return;
        }

        DataStream<String> outStream = dataStream.
                map(new ExtractSpecialties())
                .flatMap(new SplitSpecial());

        outStream.print();

        env.execute("Find Specialties");
    }

    public static class ExtractSpecialties implements MapFunction<String, String> {

        public String map(String input) throws Exception {
            try {

                return input.split(",")[1].trim();
            } catch (Exception e) {
                return null;
            }
        }

    }
    public static class SplitSpecial implements FlatMapFunction<String, String> {
        public void flatMap(String input, Collector<String> out)
                throws Exception {

            String[] specialties = input.split("\t");



            for (String specialty: specialties) {


                out.collect(specialty.trim());
            }


        }
    }

}
