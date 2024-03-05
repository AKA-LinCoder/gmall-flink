package com.echo.app.dwd;

import com.echo.utils.MyKafkaUtil;
import com.echo.utils.MysqlUtil;
import com.ibm.icu.impl.Row;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;


///数据流 ： web/app -》 nginx -> 业务服务器 (Mysql) -> maxwell -> kafka(ods) -> flinkApp -> kafka(dwd)
/// 程序 mock -> mysql ->maxwell ->kafka(zk) -> DwdTradeOrderPreProcess->kafka(zk)

public class DwdTradeOrderPreProcess  {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //设置状态ttl 生产环境设置为最大乱序程度
        tableEnvironment.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        //TODO 创建topic_db表
        tableEnvironment.executeSql(MyKafkaUtil.getTopicDb("order_preProcess"));
        //TODO 过滤出订单明细数据
        Table orderDetailTable = tableEnvironment.sqlQuery("" +
                "select " +
                "  data['id'] id, " +
                "  data['order_id'] order_id, " +
                "  data['sku_id'] sku_id, " +
                "  data['sku_name'] sku_name, " +
                "  data['order_price'] order_price, " +
                "  data['sku_num'] sku_num, " +
                "  data['create_time'] create_time, " +
                "  data['source_type'] source_type, " +
                "  data['source_id'] source_id, " +
                "  data['split_total_amount'] split_total_amount, " +
                "  data['split_activity_amount'] split_activity_amount, " +
                "  data['split_coupon_amount'] split_coupon_amount, " +
                "  `pt` " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail' ");
        tableEnvironment.createTemporaryView("order_detail_table",orderDetailTable);
        //转化为流并测试
//        tableEnvironment.toChangelogStream(orderDetailTable).print(">>>>>>");
        //TODO 过滤出订单数据
        Table orderInfoTable = tableEnvironment.sqlQuery("" +
                "select " +
                "  data['id'] id, " +
                "  data['consignee'] consignee, " +
                "  data['consignee_tel'] consignee_tel, " +
                "  data['total_amount'] total_amount, " +
                "  data['order_price'] order_price, " +
                "  data['order_status'] order_status, " +
                "  data['user_id'] user_id, " +
                "  data['payment_way'] payment_way, " +
                "  data['delivery_address'] delivery_address, " +
                "  data['order_comment'] order_comment, " +
                "  data['out_trade_no'] out_trade_no, " +
                "  data['trade_body'] trade_body, " +
                "  data['create_time'] create_time, " +
                "  data['operate_time'] operate_time, " +
                "  data['expire_time'] expire_time, " +
                "  data['process_status'] process_status, " +
                "  data['tracking_no'] tracking_no, " +
                "  data['parent_order_id'] parent_order_id, " +
                "  data['province_id'] province_id, " +
                "  data['activity_reduce_amount'] activity_reduce_amount, " +
                "  data['coupon_reduce_amount'] coupon_reduce_amount, " +
                "  data['original_total_amount'] original_total_amount, " +
                "  data['feight_fee'] feight_fee, " +
                "  data['feight_fee_reduce'] feight_fee_reduce, " +
                "  data['refundable_time'] refundable_time, " +
                "  `type`, " +
                "  `old` " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_info' ");
        tableEnvironment.createTemporaryView("order_info_table",orderInfoTable);
        //TODO 过滤出订单明细活动关联数据
        Table orderDetailActivityTable = tableEnvironment.sqlQuery("" +
                "select " +
                "  data['id'] id, " +
                "  data['order_id'] order_id, " +
                "  data['order_detail_id'] order_detail_id, " +
                "  data['activity_id'] activity_id, " +
                "  data['activity_rule_id'] activity_rule_id, " +
                "  data['sku_id'] sku_id, " +
                "  data['create_time'] create_time " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail_activity' ");
        tableEnvironment.createTemporaryView("order_detail_activity_table",orderDetailActivityTable);
        //TODO 过滤出订单明细购物券关联数据
        Table orderDetailCouponTable = tableEnvironment.sqlQuery("" +
                "select " +
                "  data['id'] id, " +
                "  data['order_id'] order_id, " +
                "  data['order_detail_id'] order_detail_id, " +
                "  data['coupon_id'] coupon_id, " +
                "  data['coupon_use_id'] coupon_use_id, " +
                "  data['sku_id'] sku_id, " +
                "  data['create_time'] create_time " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail_coupon' ");
        tableEnvironment.createTemporaryView("order_detail_coupon_table",orderDetailCouponTable);
        //TODO 创建base_dic lookup表
        tableEnvironment.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        //TODO 关联五张表
        Table resultTable = tableEnvironment.sqlQuery(""+
                "select \n" +
                "od.id,\n" +
                "od.order_id,\n" +
                "oi.user_id,\n" +
                "oi.order_status,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "oi.province_id,\n" +
                "act.activity_id,\n" +
                "act.activity_rule_id,\n" +
                "cou.coupon_id,\n" +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id,\n" +
                "od.create_time,\n" +
                "date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id,\n" +
                "oi.operate_time,\n" +
                "od.source_id,\n" +
                "od.source_type source_type_id,\n" +
                "dic.dic_name source_type_name,\n" +
                "od.sku_num,\n" +
//                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount,\n" +
                "oi.`type`,\n" +
                "oi.`old`\n" +
//                "od.od_ts,\n" +
//                "oi.oi_ts,\n" +
//                "current_row_timestamp() row_op_ts\n" +
                "from order_detail_table od \n" +
                "join order_info_table oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_detail_activity_table act\n" +
                "on od.id = act.order_detail_id\n" +
                "left join order_detail_coupon_table cou\n" +
                "on od.id = cou.order_detail_id\n" +
                "join `base_dic` for system_time as of od.pt as dic\n" +
                "on od.source_type = dic.dic_code");
        tableEnvironment.createTemporaryView("result_table", resultTable);
        tableEnvironment.toChangelogStream(resultTable).print("result:>>>>");
        //TODO 创建upsert-kafka表
        tableEnvironment.executeSql("" +
                "create table dwd_trade_order_pre_process(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "order_status string,\n" +
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
                "source_type string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
//                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "`type` string,\n" +
                "`old` map<string,string>,\n" +
//                "od_ts string,\n" +

//                "oi_ts string,\n" +
//                "row_op_ts timestamp_ltz(3),\n" +
                "primary key(id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_pre_process"));

        //TODO 将数据写出
        tableEnvironment.executeSql("insert into dwd_trade_order_pre_process select * from result_table");
        //TODO 启动任务
        environment.execute("DwdTradeOrderPreProcess");
    }
}
