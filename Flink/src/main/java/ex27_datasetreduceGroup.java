import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
// use courses.txt


// FlatMap
public class ex27_datasetreduceGroup {

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> dataSet  = env.readTextFile(params.get("input"));

        DataSet<Tuple2<String,Double>> output = dataSet.
                 map(new parseRow())
                .groupBy(0)
                .reduceGroup(new SumAndCount())
                .map(new Average());

        output.print();

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
                GroupReduceFunction<Tuple3<String, Double, Integer>,Tuple3<String, Double, Integer>> {

            public void reduce(
                   Iterable<Tuple3<String, Double, Integer>> values,
                    Collector<Tuple3<String, Double, Integer>> collector) {

                String key = null;
                Double sum = 0.0;
                Integer count = 0;

                for(Tuple3<String, Double, Integer> value:values){

                    if(key==null){
                        key = value.f0;
                    }

                    sum+=value.f1;
                    count+=value.f2;

                }


                collector.collect(Tuple3.of(key,sum,count));

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










