package flinknet;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

//  1）transient修饰的变量不能被序列化；
//  2）transient只作用于实现 Serializable 接口；
//  3）transient只能用来修饰普通成员变量字段；
//  4）不管有没有 transient 修饰，静态变量都不能被序列化；
    private transient ValueState<Boolean> flagstate;
    private transient ValueState<Long> timerstate;

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {

        timerstate.clear();
        flagstate.clear();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN);

        flagstate = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor timeDescriptor = new ValueStateDescriptor<>(
                "time-state",
                Types.LONG);

        timerstate = getRuntimeContext().getState(timeDescriptor);
    }


    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {

        Boolean lastTransactionWasSmall = flagstate.value();

        if(lastTransactionWasSmall != null){
            if (transaction.getAmount()>LARGE_AMOUNT){
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }

            cleanUp(context);
        }

        if(transaction.getAmount()<SMALL_AMOUNT){

            flagstate.update(true);

            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);

            timerstate.update(timer);
        }

    }

    private void cleanUp(Context ctx) throws Exception {

        // 删除时间
        Long timer = timerstate.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // 清除所有状态
        timerstate.clear();
        flagstate.clear();
    }
}
