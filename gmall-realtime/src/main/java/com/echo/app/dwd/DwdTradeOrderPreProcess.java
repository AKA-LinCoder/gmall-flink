package com.echo.app.dwd;

import com.echo.utils.MyKafkaUtil;
import com.echo.utils.MysqlUtil;
import com.ibm.icu.impl.Row;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderPreProcess  {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
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
                tableEnvironment.toChangelogStream(orderDetailActivityTable).print(">>>>>>");
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
                tableEnvironment.toChangelogStream(orderDetailCouponTable).print(">>>>>>");
        //TODO 创建base_dic lookup表
        tableEnvironment.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        //TODO 关联五张表
        //TODO 创建upsert-kafka表
        //TODO 将数据写出
        //TODO 启动任务
        environment.execute("");
    }
}
