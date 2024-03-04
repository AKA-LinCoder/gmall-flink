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
                "  pt " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail' ");
//        tableEnvironment.createTemporaryView("order_detail_table",orderDetailTable);
        //转化为流并测试
        tableEnvironment.toChangelogStream(orderDetailTable).print(">>>>>>");
        //TODO 过滤出订单数据
        //TODO 过滤出订单明细活动关联数据
        //TODO 过滤出订单明细购物券关联数据
        //TODO 创建base_dic lookup表
        tableEnvironment.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        //TODO 关联五张表
        //TODO 创建upsert-kafka表
        //TODO 将数据写出
        //TODO 启动任务
        environment.execute("");
    }
}
