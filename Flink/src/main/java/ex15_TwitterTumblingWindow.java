import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Properties;


public class ex15_TwitterTumblingWindow {

    public static void main(String[] args) throws Exception {


        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "");
        props.setProperty(TwitterSource.TOKEN, "");
        props.setProperty(TwitterSource.TOKEN_SECRET, "");
        DataStream<String> streamSource = env.addSource(new TwitterSource(props));

       // streamSource.print();

        DataStream<Tuple2<String, Integer>> tweets = streamSource
                .map(new LocationCount())
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1);

        tweets.print();

        // execute program
        env.execute("Twitter Location Count");
    }

    public static class LocationCount
            implements MapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper jsonParser;

        public Tuple2<String, Integer> map(String value) throws Exception {
            if(jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            String location = jsonNode.has("user") && jsonNode.get("user").has("location")
                    ? jsonNode.get("user").get("location").getValueAsText()
                    : "unknown";
            return new Tuple2<String, Integer>(location, 1);
        }
    }
}
