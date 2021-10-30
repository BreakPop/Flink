package myflink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;


import static org.apache.flink.table.api.Expressions.$;


public class TableBatchDemo {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSource<MyOrder> input = env.fromElements(
                new MyOrder(1L, "BMW", 1),
                new MyOrder(2L, "Tesla", 8),
                new MyOrder(2L, "Tesla", 8),
                new MyOrder(3L, "Rolls-Royce", 20));


        Table table = tEnv.fromDataSet(input);

        Table filtered = table
                    .where($("amount")
                    .isGreaterOrEqual(8));

        System.out.println(filtered.explain());

        DataSet<MyOrder> result = tEnv.toDataSet(filtered, MyOrder.class);

        result.print();

    }
}
