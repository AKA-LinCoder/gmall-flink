package com.echo.utils;

import com.alibaba.fastjson.JSONObject;
import com.echo.bean.KeywordBean;
import com.echo.bean.TransientSink;
import com.echo.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {
    public static <T> SinkFunction<T> getSinkFunction(String sql){

        return JdbcSink.<T>sink(sql, new JdbcStatementBuilder<T>() {
            @Override
            public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                System.out.println("666");
                //使用反射的方式获取t对象的数据
                Class<?> tClass = t.getClass();

//                Method[] methods = tClass.getMethods();
//                for (int i = 0; i < methods.length; i++) {
//                    Method method = methods[i];
//                    method.invoke(t);
//                }
                Field[] fields = tClass.getDeclaredFields();
                int offset = 0;
                //遍历属性
                for (int i = 0; i < fields.length; i++) {
                    //获取单个属性
                    Field field = fields[i];
                    field.setAccessible(true);

                    //尝试获取字段上的自定义注解
                    TransientSink annotation =  field.getAnnotation(TransientSink.class);
                    if(annotation != null){
                        offset++;
                        continue;
                    }
                    try {
                        //获取属性值
                        Object value = field.get(t);
                        //给占位赋值
                        preparedStatement.setObject(i+1-offset,value);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        },new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                .build(),new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withPassword("Estim@b509")
                        .withUsername("default")
                .build());
    }
}
