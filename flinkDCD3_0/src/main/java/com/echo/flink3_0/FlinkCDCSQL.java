package com.echo.flink3_0;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDCSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        //TODO 使用flinkCDC SQL方式建表
        tableEnvironment.executeSql("" +
                "create table t1(\n"+
                " id string primary key NOT ENFORCED,\n"+
                " name string"+
                ") WITH (\n"+
                " 'connector' = 'mysql-cdc',\n"+
                " 'hostname' = '192.168.1.133',\n"+
                " 'port' = '3306',\n"+
                " 'username' = 'root',\n"+
                " 'password' = 'Estim@b509',\n"+
                " 'database-name' = 'test_flink_cdc',\n"+
                " 'table-name' = 't1'\n"+
                ")");

        Table table = tableEnvironment.sqlQuery("select * from t1");

        table.execute().print();
    }
}
