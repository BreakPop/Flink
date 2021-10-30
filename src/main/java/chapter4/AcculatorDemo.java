package chapter4;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;


// 实现累加器
public class AcculatorDemo {
    // main()方法——Java应用程序的入口
    public static void main(String[] args) throws Exception{
        // 获取执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 加载或创建数据源
        DataSource<String> input = env.fromElements("BMW", "Tesla", "Rolls-Royce","啦啦啦");
        // 转换数据
        DataSet<String> result = input.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                // 使用累加器。 并行度为1，普通累加求和即可。 设置并行度，普通累加不准（？）

                intCounter.add(1);
                return value;
            }

            // 创建累加器
            IntCounter intCounter = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 注册累加器
                getRuntimeContext().addAccumulator("myAccumulatorName", intCounter);
            }
        });

        result.writeAsText("./file.TXT", FileSystem.WriteMode.OVERWRITE)
                // 设置并行度
                .setParallelism(1);
        // 作业执行结果对象
        JobExecutionResult jobExecutionResult = env.execute("myJob");
        // 获取累加器结果，参数是累加器的名称，不是intCounter的名称
        Object myAccumulatorName = jobExecutionResult.getAccumulatorResult("myAccumulatorName");
        System.out.println(myAccumulatorName);
    }
}
