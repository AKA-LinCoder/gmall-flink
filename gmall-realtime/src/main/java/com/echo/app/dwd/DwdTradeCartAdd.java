package com.echo.app.dwd;

import com.echo.utils.MyKafkaUtil;

import com.echo.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;



public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        environment.setRestartStrategy( RestartStrategies.fixedDelayRestart(1, Time.of(3, TimeUnit.SECONDS)));
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //TODO 使用DDL方式读取topic_db 主题数据创建表
        tableEnvironment.executeSql(MyKafkaUtil.getTopicDb("CartAdd")).print();
        //TODO 过滤处加购数据
        Table cartAdd = tableEnvironment.sqlQuery("" +
                "select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['sku_name'] sku_name,\n" +
                "data['cart_price'] cart_price,\n" +
                "data['source_id'] source_id,\n" +
                "data['source_type'] source_type,\n" +
                "data['is_checked'] is_checked,\n" +
                "data['create_time'] create_time,\n" +
                "data['operate_time'] operate_time,\n" +
                "data['is_ordered'] is_ordered,\n" +
                "data['order_time'] order_time,\n" +
                "data['source_type'] source_type,\n" +
                "data['source_id'] source_id,\n" +
                "if(`type` = 'insert',\n" +
                "data['sku_num'],cast((cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string)) sku_num,\n" +
                "pt\n" +
//                "proc_time\n" +
                "from `topic_db` \n" +
                "where `database` = 'gmall'\n"+
                "and `table` = 'cart_info'\n" +
                "and (`type` = 'insert'\n" +
                "or (`type` = 'update' \n" +
                "and `old`['sku_num'] is not null \n" +
                "and cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int)))");

        //将架构表转化为流并打印测试 创建下面会使用的表名
//        tableEnvironment.toChangelogStream(cartAdd).print(">>>>>");
        tableEnvironment.createTemporaryView("cart_info_table", cartAdd);
        //TODO 读取mysql的base_dic表作为lookup表
        tableEnvironment.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        //TODO 关联两张表 必须空一定的距离
        Table cartAddWithDicTable = tableEnvironment.sqlQuery("" +
                "select " +
                "   ci.id, " +
                "   ci.user_id, " +
                "   ci.sku_id, " +
                "   ci.cart_price, " +
                "   ci.sku_num, " +
                "   ci.sku_name, " +
                "   ci.is_checked, " +
                "   ci.create_time, " +
                "   ci.operate_time, " +
                "   ci.is_ordered, " +
                "   ci.order_time, " +
                "   ci.source_type source_type_id, " +
                "   dic.dic_name  source_type_name, " +
                "   ci.source_id " +
                "from cart_info_table  ci " +
                "join base_dic FOR SYSTEM_TIME AS OF ci.pt as dic " +
                "on ci.source_type = dic.dic_code");
        //TODO 使用DDL 方式创建加购事实表
        tableEnvironment.executeSql(""+
                "create table dwd_cart_add(" +
                "   `id` STRING,  " +
                "   `user_id` STRING,  " +
                "   `sku_id` STRING,  " +
                "   `cart_price` STRING,  " +
                "   `sku_num` STRING,  " +
                "   `sku_name` STRING,  " +
                "   `is_checked` STRING,  " +
                "   `create_time` STRING, " +
                "   `operate_time` STRING,  " +
                "   `is_ordered` STRING,  " +
                "   `order_time` STRING,  " +
                "   `source_type_id` STRING,  " +
                "   `source_type_name` STRING,  " +
                "   `source_id` STRING  " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add")
        ) ;
        //TODO 将数据写出
        tableEnvironment.executeSql("insert into dwd_cart_add select * from " +cartAddWithDicTable).print();
        //TODO 启动任务
        environment.execute("DwdTradeCartAdd");
    }
}
