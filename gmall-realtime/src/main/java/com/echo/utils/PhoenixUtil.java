package com.echo.utils;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.echo.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {

    //phoenix中新增和修改使用的都是upsert

    /**
     *
     * @param connection phoenix连接
     * @param sinkTable 表名 eg:tn
     * @param data 数据 eg：{"id":"1001","name":"mike","sex":"male"}
     */
    public static void upsertValues(DruidPooledConnection connection, String sinkTable, JSONObject data) throws SQLException {
        //1 拼接sql upsert into db.tn(id,name,sex) values('1001','mike','male')
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        //StringUtils.join(columns,",") -> "id,name,sex"
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable +"(" + StringUtils.join(columns,",")
                +") values ('"+ StringUtils.join(values,"','")+"')";

        //2 预编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        //3 执行
        preparedStatement.execute();
        connection.commit();
        //4 释放资源
        preparedStatement.close();
    }
}
