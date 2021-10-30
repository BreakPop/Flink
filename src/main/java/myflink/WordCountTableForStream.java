package myflink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class WordCountTableForStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, bsSettings);

        DataStream<String> dataStream = sEnv.addSource(new MySource());

        Table table1 = tEnv.fromDataStream(dataStream, $("word"));

        Table table = table1
                .where($("word")
                .like("%t%"));

        System.out.println(table.explain());

        tEnv.toAppendStream(table, Row.class)
                .print("table");
        sEnv.execute();


    }
}
