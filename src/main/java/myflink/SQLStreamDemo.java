package myflink;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class SQLStreamDemo {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<String> stream = env.addSource(new MySource());

        Table table1 = tEnv.fromDataStream(stream, $("word"));

        Table result = tEnv.sqlQuery("SELECT * FROM " + table1 + " WHERE word LIKE '%t%'");

        tEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }
}
