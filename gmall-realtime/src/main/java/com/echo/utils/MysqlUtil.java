package com.echo.utils;

public class MysqlUtil {
    public static String getBaseDicLookUpDDL() {

        return "create table `base_dic`(\n" +
                "`dic_code` string,\n" +
                "`dic_name` string,\n" +
                "`parent_code` string,\n" +
                "`create_time` timestamp,\n" +
                "`operate_time` timestamp,\n" +
                "primary key(`dic_code`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }

    public static String mysqlLookUpTableDDL(String tableName) {

        //解决No appropriate protocol (protocol is disabled or cipher suites are inappropriate)
        //useSSL=false
        String ddl = "WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:mysql://192.168.10.102:3306/gmall?useSSL=false',\n" +
                "'table-name' = '" + tableName + "',\n" +
                "'lookup.cache.max-rows' = '10',\n" +
                "'lookup.cache.ttl' = '1 hour',\n" +
                "'username' = 'root',\n" +
                "'password' = 'Estim@b509',\n" +
                "'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")";
        return ddl;
    }
}