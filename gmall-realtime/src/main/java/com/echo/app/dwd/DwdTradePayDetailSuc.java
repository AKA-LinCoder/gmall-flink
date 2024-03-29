package com.echo.app.dwd;

import com.echo.utils.MyKafkaUtil;
import com.echo.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

///数据流 ： web/app -》 nginx -> 业务服务器 (Mysql) -> maxwell -> kafka(ods) -> flinkApp -> kafka(dwd) -> flinkapp -> kafka(dwd)  -> flinkapp -> kafka(dwd)
/// 程序 mock -> mysql ->maxwell ->kafka(zk) -> DwdTradeOrderPreProcess->kafka(zk) -> DwdTradeOrderDetail->kafka(zk) -> DwdTradePayDetailSuc->kafka(zk)
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
//        tableEnvironment.toChangelogStream(paymentInfo).print();
        //消费下单主题数据
        tableEnvironment.executeSql("" +
                "create table dwd_trade_order_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
//                "sku_num string,\n" +
                "order_price string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
//                "date_id string,\n" +
                "create_time string,\n" +
                "source_id string,\n" +
                "source_type_id string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
//                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string\n" +
//                "ts string,\n" +
//                "row_op_ts timestamp_ltz(3)\n" +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_detail","trade_pay_detail_suc"));
        //读取mysql base_dic表
        tableEnvironment.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        //三表 关联
        Table resultTable = tableEnvironment.sqlQuery("" +
                "select\n" +
                "od.id order_detail_id,\n" +
                "od.order_id,\n" +
                "od.user_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "od.province_id,\n" +
                "od.activity_id,\n" +
                "od.activity_rule_id,\n" +
                "od.coupon_id,\n" +
                "pi.payment_type payment_type_code,\n" +
                "dic.dic_name payment_type_name,\n" +
                "pi.callback_time,\n" +
                "od.source_id,\n" +
                "od.source_type_id,\n" +
                "od.source_type_name,\n" +
                "od.sku_num,\n" +
                "od.order_price,\n" +
//                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount split_payment_amount\n" +
//                "pi.ts,\n" +
//                "od.row_op_ts row_op_ts\n" +
                "from payment_info pi\n" +
                "join dwd_trade_order_detail od\n" +
                "on pi.order_id = od.order_id\n" +
                "join `base_dic` for system_time as of pi.pt as dic\n" +
                "on pi.payment_type = dic.dic_code");
        tableEnvironment.createTemporaryView("result_table", resultTable);
        //创建Kafka支付成功表
        tableEnvironment.executeSql("create table dwd_trade_pay_detail_suc(\n" +
                "order_detail_id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "payment_type_code string,\n" +
                "payment_type_name string,\n" +
                "callback_time string,\n" +
                "source_id string,\n" +
                "source_type_id string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "order_price string,\n" +
//                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_payment_amount string,\n" +
//                "ts string,\n" +
//                "row_op_ts timestamp_ltz(3),\n" +
                "primary key(order_detail_id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_detail_suc"));
        //将数据写出
        tableEnvironment.executeSql("" +
                "insert into dwd_trade_pay_detail_suc select * from result_table");
        //启动任务
        environment.execute("");
    }
}
