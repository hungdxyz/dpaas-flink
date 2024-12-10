import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ex26_split {

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
        SplitStream<String> split = dataStream.split(new rerouteData());
        DataStream<String> awords = split.select("Awords");
        DataStream<String> otherwords = split.select("Others");

        awords.writeAsText("/Users/swethakolalapudi/flinkJar/Split1.txt", FileSystem.WriteMode.OVERWRITE);
        otherwords.writeAsText("/Users/swethakolalapudi/flinkJar/Split2.txt", FileSystem.WriteMode.OVERWRITE);


        env.execute("Window co-Group Example");
    }




    public static class rerouteData implements OutputSelector<String>{

        public Iterable<String> select(String s) {
            List<String> outputs = new ArrayList<String>();

            if (s.startsWith("a")) {
                outputs.add("Awords");
            } else {
                outputs.add("Others");
            }

            return outputs;
        }
    }






}
