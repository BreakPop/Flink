package chapter7;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TublingCountWindowDemo {
    // main()--方法，Java程序的入口
    public static void main(String[] args) throws Exception{
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 自定义数据源
        DataStreamSource<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("S1", 1),
                Tuple2.of("S1", 6),
                Tuple2.of("S1", 4),
                Tuple2.of("S2", 2),
                Tuple2.of("S2", 5),
                Tuple2.of("S3", 8),
                Tuple2.of("S3", 10),
                Tuple2.of("S3", 8),
                Tuple2.of("S4", 9),
                Tuple2.of("S4", 6),
                Tuple2.of("S4", 12),
                Tuple2.of("S4", 13),
                Tuple2.of("S4", 6),
                Tuple2.of("S4", 12),
                Tuple2.of("S4", 13)

        );

        input.keyBy(value ->value.f0)

                // 满足三个才会计数 否则不会出现在控制台
                .countWindow(3)
                .sum(1)
                .print();

        env.execute();
    }
}
