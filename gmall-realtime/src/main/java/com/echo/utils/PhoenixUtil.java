package com.echo.utils;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.echo.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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


    /**
     * Phoenix 表查询方法
     * @param conn 数据库连接对象
     * @param sql 查询数据的 SQL 语句
     * @param clz 返回的集合元素类型的 class 对象
     * @param <T> 返回的集合元素类型
     * @return 封装为 List<T> 的查询结果
     */
    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz) {
        List<T> resList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();

            /**处理结果集
             +-----+----------+
             | ID  | TM_NAME  |
             +-----+----------+
             | 17  | lzls     |
             | 18  | mm       |

             class TM{id,tm_name}
             */
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()){
                //通过反射，创建对象，用于封装查询结果
                T obj = clz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    BeanUtils.setProperty(obj,columnName,columnValue);
                }
                resList.add(obj);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从phoenix数据库中查询数据发送异常了~~");
        } finally {
            //释放资源
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }
}
