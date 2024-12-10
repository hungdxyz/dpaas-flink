import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class ex24_coMap {

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);


        DataStream<String> stream1 = env.socketTextStream("localhost", 8000);
        DataStream<String> stream2 = env.socketTextStream("localhost", 9000);

        DataStream<Tuple2<String, Double>> coMapStream =
                stream1.connect(stream2).
                        map(new parseRow());

        coMapStream.print();

        env.execute("Window co-Group Example");
    }




    public static class parseRow implements CoMapFunction<String,String,Tuple2<String, Double>>{

        public Tuple2<String, Double> map1(String s) throws Exception {

            String[] data = s.split(" ");
            return Tuple2.of(data[0].trim(),Double.parseDouble(data[1].trim())) ;
        }


        public Tuple2<String, Double> map2(String s) throws Exception {

            String[] data = s.split(" ");
            return Tuple2.of(data[0].trim(),Double.parseDouble(data[1].trim())) ;
        }
    }





}
