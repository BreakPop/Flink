package flinknet;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT,"8081-8089");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        DataStream <Transaction> transactions=env
                .addSource(new TransactionSource())
                .name("transactions");

        DataStream <Alert> alerts=transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");
        alerts
                .addSink(new AlertSink())
                .name("send-alerts");


        env.execute("Fraud Detection");
    }
}
