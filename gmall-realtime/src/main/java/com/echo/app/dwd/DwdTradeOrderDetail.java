package com.echo.app.dwd;

import com.echo.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


///数据流 ： web/app -》 nginx -> 业务服务器 (Mysql) -> maxwell -> kafka(ods) -> flinkApp -> kafka(dwd) -. flinkapp -> kafka(dwd)
/// 程序 mock -> mysql ->maxwell ->kafka(zk) -> DwdTradeOrderPreProcess->kafka(zk) -> DwdTradeOrderDetail->kafka(zk)

public class DwdTradeOrderDetail  {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //读取Kafka订单预处理主题数据
        tableEnvironment.executeSql("" +
                "create table dwd_trade_order_pre_process(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "order_status string,\n" +
                "order_price string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "operate_date_id string,\n" +
                "operate_time string,\n" +
                "source_id string,\n" +
                "source_type_id string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
//                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "`type` string,\n" +
                "`old` map<string,string> \n" +
//                "od_ts string,\n" +

//                "oi_ts string,\n" +
//                "row_op_ts timestamp_ltz(3),\n" +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_pre_process","trade_order_detail"));

        //过滤出下单数据 即新增数据
        Table filteredTable = tableEnvironment.sqlQuery("" +
                "select " +
                "id,\n" +
                "order_id,\n" +
                "user_id,\n" +
                "sku_id,\n" +
                "sku_name,\n" +
//                "sku_num,\n" +
                "order_price,\n" +
                "province_id,\n" +
                "activity_id,\n" +
                "activity_rule_id,\n" +
                "coupon_id,\n" +
//                "date_id,\n" +
                "create_time,\n" +
                "source_id,\n" +
//                "source_type source_type_code,\n" +
                "source_type_id,\n" +
                "source_type_name,\n" +
                "sku_num,\n" +
//                "split_original_amount,\n" +
                "split_activity_amount,\n" +
                "split_coupon_amount,\n" +
                "split_total_amount\n" +
//                "od_ts ts,\n" +
//                "row_op_ts\n" +
                "from dwd_trade_order_pre_process " +
                "where `type`='insert'");
        tableEnvironment.createTemporaryView("filtered_table", filteredTable);
        //创建dwd层下单数据表
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
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_order_detail"));
        //将数据写出
        tableEnvironment.executeSql("insert into dwd_trade_order_detail select * from filtered_table");
        //启动任务
        environment.execute("DwdTradeOrderDetail");
    }
}
