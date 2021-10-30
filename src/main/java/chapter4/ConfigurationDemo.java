package chapter4;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;


// 通过withParameters()方法传递和使用参数
public class ConfigurationDemo {
    // main方法
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 加载或创建数据源
        DataSet<Integer> input = env.fromElements(1, 2, 3, 5, 10, 12, 17, 20);
        // 用Configuration类存储参数
        Configuration conf = new Configuration();
        conf.setInteger("limit", 14);
        input.filter(new RichFilterFunction<Integer>() {
            private int limit;

            @Override
            public void open(Configuration configuration) throws Exception {
                limit = configuration.getInteger("limit", 0);
            }

            @Override
            public boolean filter(Integer value) throws Exception {
                // 返回大于limit的值
                return value > limit;
            }
        }).withParameters(conf)
                .print();
    }
}
