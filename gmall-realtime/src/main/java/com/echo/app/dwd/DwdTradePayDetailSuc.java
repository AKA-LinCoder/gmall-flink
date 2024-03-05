package com.echo.app.dwd;

import com.echo.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradePayDetailSuc  {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.getConfig().setIdleStateRetention(Duration.ofSeconds(905));
        //读取topicDB数据并过滤出支付成功数据
        tableEnvironment.executeSql(MyKafkaUtil.getTopicDb("trade_pay_detail_suc"));
        Table paymentInfo = tableEnvironment.sqlQuery("select\n" +
                "data['user_id'] user_id,\n" +
                "data['order_id'] order_id,\n" +
                "data['payment_type'] payment_type,\n" +
                "data['callback_time'] callback_time,\n" +
                "`pt`\n" +
                "from topic_db\n" +
                "where `table` = 'payment_info'\n"
                +
                "and `type` = 'update'\n" +
                "and data['payment_status']='1602'"
        );
        tableEnvironment.createTemporaryView("payment_info", paymentInfo);
        tableEnvironment.toChangelogStream(paymentInfo).print();
        //消费下单主题数据
        //读取mysql base_dic表
        //三表关联
        //创建Kafka支付成功表
        //将数据写出
        //启动任务
        environment.execute("");
    }
}
