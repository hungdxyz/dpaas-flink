import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class ex23_coGroup {

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);


        DataStream<Tuple2<String, Double>> stream1 = env.socketTextStream("localhost", 8000).map(new parseRow());
        DataStream<Tuple2<String,Double>> stream2 = env.socketTextStream("localhost", 9000).map(new parseRow());

        DataStream<Tuple2<String, Double>> coGroupStream =
                stream1.coGroup(stream2).
                where(new StreamKeySelector()).equalTo(new StreamKeySelector()).
                window(TumblingProcessingTimeWindows.of(Time.seconds(30))).
                apply(new AverageProductivity());

        coGroupStream.print();

        env.execute("Window co-Group Example");
    }


    private static class StreamKeySelector implements
            KeySelector<Tuple2<String, Double>, String> {
        public String getKey(Tuple2<String, Double> value) {
            return value.f0;
        }
    }



    public static class parseRow implements MapFunction<String,Tuple2<String,Double>>{

        public Tuple2<String, Double> map(String s) throws Exception {

            String[] data = s.split(" ");
            return Tuple2.of(data[0].trim(),Double.parseDouble(data[1].trim())) ;
        }
    }




    private static class AverageProductivity implements
            CoGroupFunction<Tuple2<String, Double>,
                    Tuple2<String, Double>,
                    Tuple2<String, Double>> {


        public void coGroup(Iterable<Tuple2<String, Double>> iterable,
                            Iterable<Tuple2<String, Double>> iterable1,
                            Collector<Tuple2<String, Double>> collector) throws Exception {

            String key = null ;
            Double hours = 0.0;
            Double produced = 0.0;

            for(Tuple2<String, Double> hoursRow: iterable){
                if(key==null){
                    key = hoursRow.f0;
                }

                hours = hours+hoursRow.f1;

            }

            for(Tuple2<String, Double> producedRow: iterable1){

                produced = produced+producedRow.f1;

            }


            collector.collect(Tuple2.of(key,produced/hours));


        }
    }

}
