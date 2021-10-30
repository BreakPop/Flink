package myflink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class StreamWorldCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env.addSource(new MySource())
                .flatMap(new Splitter())
                .keyBy(x -> x.f0)
                // Processing or Event
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1);

        dataStream.print();

        System.out.println(env.getExecutionPlan());

        env.execute();

    }


    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {

            for (String word : s.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }

        }
    }
}
