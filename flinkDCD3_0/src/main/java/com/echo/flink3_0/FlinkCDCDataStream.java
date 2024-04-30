package com.echo.flink3_0;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCDataStream {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //TODO 开启checkpoint
        environment.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        //TODO 使用flinkCDC构建MySqlSource
        MySqlSource<String> build = MySqlSource.<String>builder()
                .hostname("192.168.1.133")
                .port(3306)
                .username("root")
                .password("Estim@b509")
                .databaseList("test_flink_cdc")
                .tableList("test_flink_cdc.t1")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        //TODO 读取数据
        DataStreamSource<String> mysqlSource = environment.fromSource(build, WatermarkStrategy.<String>noWatermarks(), "mysqlSource");
        //TODO 打印
        mysqlSource.print(">>>>");
        //启动
        environment.execute();
    }
}
