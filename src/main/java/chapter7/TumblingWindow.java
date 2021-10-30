package chapter7;

import myflink.MySource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class TumblingWindow {
    public static void main(String[] args) throws Exception {

        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取自定义的数据流
        DataStreamSource<String> input = env.addSource(new MySource());
        SingleOutputStreamOperator<Tuple2<String, Integer>> output = input.flatMap(new Splitter())
                .keyBy(value -> value.f0)

                // 指定窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        // 打印数据到控制台
        output.print("window");

        env.execute("wordcount");
    }

    private static class Splitter implements FlatMapFunction<String,Tuple2<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: value.split(" ") ){
                out.collect(new Tuple2<>(word,1));
            }


        }
    }


    // 使用FlatMapFunction切割字符串

}
