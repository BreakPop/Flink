package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class BatchWordCount {

    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements(
                "Flink batch demo",
                "batch demo",
                "demo"
        );

        DataSet<Tuple2<String, Integer>> ds = text.flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        ds.print();


    }

    static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word: s.split(" ")){
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
