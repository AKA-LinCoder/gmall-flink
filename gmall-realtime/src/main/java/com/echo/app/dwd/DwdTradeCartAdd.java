package com.echo.app.dwd;

import com.echo.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DwdTradeCartAdd {
    public static void main(String[] args) {
        //TODO 获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //TODO 使用DDL方式读取topic_db 主题数据创建表
        tableEnvironment.executeSql(MyKafkaUtil.getTopicDb("CartAdd"));
        //TODO 过滤处加购数据
        Table cartAdd = tableEnvironment.sqlQuery("" +
                "select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['source_id'] source_id,\n" +
                "data['source_type'] source_type,\n" +
                "if(`type` = 'insert',\n" +
                "data['sku_num'],cast((cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string)) sku_num,\n" +
                "ts,\n" +
                "proc_time\n" +
                "from `topic_db` \n" +
                "where `database` = 'gmall'\n"+
                "and `table` = 'cart_info'\n" +
//                "where `table` = 'cart_info'\n" +
                "and (`type` = 'insert'\n" +
                "or (`type` = 'update' \n" +
                "and `old`['sku_num'] is not null \n" +
                "and cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int)))");
        //将架构表转化为流并打印测试
        tableEnvironment.toAppendStream(cartAdd, Row.class).print(">>>>>");
//        tableEnvironment.createTemporaryView("cart_add", cartAdd);
        //TODO 读取mysql的base_dic表作为lookup表
        //TODO 关联两张表
        //TODO 使用DDL 方式创建加购事实表
        //TODO 将数据写出
        //TODO 启动任务
    }
}
