package com.echo.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.echo.utils.DruidDSUtil;
import com.echo.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    ///连接池
    private static DruidDataSource druidDataSource = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();
        //写出数据
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        //如果这里出现异常，有两种后续，1 直接挂掉 2 继续执行
        // 1 直接挂掉 ，如果只考虑从phoenix中读取数据 可以直接挂掉
        // 2 继续执行 ，如果考虑phoenix中读取不到数据，要再从mysql中读取数据的话，就可以继续执行
        PhoenixUtil.upsertValues(connection,sinkTable,data);
        //归还连接
        connection.close();
    }
}
