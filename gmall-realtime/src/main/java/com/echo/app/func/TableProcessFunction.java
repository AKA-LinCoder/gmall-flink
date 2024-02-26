package com.echo.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.echo.bean.TableProcess;
import com.echo.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {


    private Connection connection;

    private MapStateDescriptor<String,TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }


    @Override
    public void open(Configuration parameters)  {
        //初始化 Phoenix 的连接
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        }catch (Exception e){
            throw new RuntimeException("连接phoenix失败 -> " + e.getMessage());
        }
    }


    /***
     * 处理广播流数据
     * @param s
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        System.out.println("我要处理广播流了");
        //处理广播流
        //1 获取并解析数据，方便主流操作
        JSONObject jsonObject = JSON.parseObject(s);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);
        //2 校验表是否存在，如果不存在则需要在phoenix中创建表
        checkTable(tableProcess.getSinkTable(),tableProcess.getSinkColumns(),tableProcess.getSinkPk(),tableProcess.getSinkExtend());
        //3 写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(),tableProcess);

    }

    /**
     * 校验并建表
     * @param sinkTable
     * @param sinkColumns
     * @param sinkPk
     * @param sinkExtend
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //处理特殊字段
        if(sinkPk==null||"".equals(sinkPk)){
            sinkPk = "id";
        }

        if(sinkExtend==null){
            sinkExtend="";
        }
        PreparedStatement preparedStatement = null;

     try {
         //拼接sql
         StringBuffer createTableIfSql = new StringBuffer("create table if not exists ")
                 .append(GmallConfig.HBASE_SCHEMA)
                 .append(".")
                 .append(sinkTable)
                 .append("(");
         String[] split = sinkColumns.split(",");
         for (int i = 0;i<split.length;i++){
             //取出字段
             String column = split[i];
             //判断是否是主键
             if(sinkPk.equals(column)){

                 createTableIfSql.append(column).append(" varchar primary key");

             }else {
                 createTableIfSql.append(column).append(" varchar");
             }
             //判断是否是最后一个字段
             if(i<split.length-1){
                 createTableIfSql.append(",");
             }
         }
         createTableIfSql.append(")").append(sinkExtend);
         //编译sql
         System.out.println("建表语句"+createTableIfSql);
         preparedStatement = connection.prepareStatement(createTableIfSql.toString());
         //执行sql,建表
         preparedStatement.execute();

     }catch (SQLException sqlException){
         throw new  RuntimeException("建表失败" + sinkTable+sqlException.getMessage());
     } finally {

         //释放资源
         if(preparedStatement!=null){

             try {
                 //释放资源
                 preparedStatement.close();
             }catch (SQLException e){
                 e.printStackTrace();
             }
         }
     }


    }

    /***
     * 主流数据处理
     * @param jsonObject
     * @param readOnlyContext
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //处理主流数据
        //1 获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String table = jsonObject.getString("table");
        TableProcess tableProcess = broadcastState.get(table);

        if(tableProcess!=null){
            //2 过滤字段
            filterColumn(jsonObject.getJSONObject("data"),tableProcess.getSinkColumns());
            //3 补充sinkTable字段输出
            jsonObject.put("sinkTable",tableProcess.getSinkTable());
            collector.collect(jsonObject);

        } else{

            System.out.println("找不到对应的key"+table);
        }



    }

    /**
     * 过滤数据
     * @param data {"id":13,"tm_name":"dasdas","logo":"xxx"}
     * @param sinkColumns "id,tm_name"
     */
    private void filterColumn(JSONObject data, String sinkColumns) {


        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);


        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//        while ((iterator.hasNext())){
//            Map.Entry<String, Object> next = iterator.next();
//            if(!sinkColumns.contains(next.getKey())){
//                iterator.remove();
//            }
//        }

    }


}
