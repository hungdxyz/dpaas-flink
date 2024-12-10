import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import  java.util.List;

// KeyedStreams, stateful RichFlatMap
public class ex17_WordCountState {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> collectStream = env.socketTextStream("localhost", 8000);



        DataStream<String> wordCountStream = collectStream
                .map(new wordToTuple())
                .keyBy(0)
                .flatMap(new collectTotalWordCount());

        wordCountStream.print();

        env.execute("Word Count");
    }

    public static class wordToTuple implements MapFunction<String, Tuple2<Integer,String>> {
        public Tuple2<Integer,String> map(String input) throws Exception {

            return Tuple2.of(1,input.trim().toLowerCase());
        }
    }
    public static class collectWordCounts
            extends RichFlatMapFunction<Tuple2<Integer,String>, String> {

        private transient ValueState<Map<String, Integer>> allWordCounts;

        public void flatMap(Tuple2<Integer,String> input, Collector<String> out)
                throws Exception {


            Map<String, Integer> currentWordCounts = allWordCounts.value();


            if(input.f1.equals("print")){
                out.collect(currentWordCounts.toString());
                allWordCounts.clear();

            }

else {
                if (!currentWordCounts.containsKey(input.f1)) {
                    currentWordCounts.put(input.f1, 1);
                } else {

                    Integer wordCount = currentWordCounts.get(input.f1);
                    currentWordCounts.put(input.f1, 1 + wordCount);
                }

                allWordCounts.update(currentWordCounts);

            }

        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Map<String, Integer>> descriptor =
                    new ValueStateDescriptor<Map<String, Integer>>(
                            // the state name
                            "allWordCounts",
                            // type information
                            TypeInformation.of(new TypeHint<Map<String, Integer>>(){}),
                            // default value of the state, if nothing was set
                            new HashMap<String, Integer>());
            allWordCounts = getRuntimeContext().getState(descriptor);
        }
    }


    public static class collectDistinctWords
            extends RichFlatMapFunction<Tuple2<Integer,String>, String> {

        private transient ListState<String> distinctWordList;

        public void flatMap(Tuple2<Integer,String> input, Collector<String> out)
                throws Exception {
            Iterable<String> currentWordList = distinctWordList.get();
            Boolean oldWord = false;

            if(input.f1.equals("print")){
                out.collect(currentWordList.toString());
                distinctWordList.clear();
            }

            else {
                for (String word : currentWordList) {
                    if (input.f1.equals(word)) {
                        oldWord = true;
                        break;
                    }
                }

                    if(!oldWord){
                        distinctWordList.add(input.f1);
                    }
                }


            }

        @Override
        public void open(Configuration config) {
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<String>(
                    "wordList", String.class);
            distinctWordList = getRuntimeContext().getListState(descriptor);
        }
    }

    public static class collectTotalWordCount
            extends RichFlatMapFunction<Tuple2<Integer,String>, String> {


        private transient ReducingState<Integer> totalCountState;

        public void flatMap(Tuple2<Integer,String> input, Collector<String> out)
                throws Exception {

            if (input.f1.equals("print")) {
                out.collect(totalCountState.get().toString());
                totalCountState.clear();
            } else {

                totalCountState.add(1);
            }
        }
        @Override
        public void open(Configuration config) {

            ReducingStateDescriptor<Integer> reducingStateDescriptor =
                    new ReducingStateDescriptor<Integer>(
                            "totalCount",
                            new ReduceFunction<Integer>() {
                                public Integer reduce(Integer cumulative, Integer input) {
                                    return cumulative + input;
                                }
                            },
                            Integer.class);

            totalCountState = getRuntimeContext().getReducingState(reducingStateDescriptor);
        }
    }

}
