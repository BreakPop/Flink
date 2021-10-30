package myflink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Random;

public class MySource implements SourceFunction<String> {

    private long count = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            //单词流
            ArrayList<String> stringList = new ArrayList<>();
            stringList.add("world");
            stringList.add("Flink");
            stringList.add("Steam");
            stringList.add("Batch");
            stringList.add("Table");
            stringList.add("SQL");
            stringList.add("hello");
            int size = stringList.size();
            int i = new Random().nextInt(size);
            ctx.collect(stringList.get(i));
            System.out.println("Source:" + stringList.get(i));
            // 每秒产生一条数据
            Thread.sleep(1000);

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
