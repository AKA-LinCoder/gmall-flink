package com.echo.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.echo.app.func.TableProcessFunction;
import com.echo.bean.TableProcess;
import com.echo.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.lang.module.Configuration;
import java.util.Properties;


public class DimApp {


    public static void main(String[] args) throws Exception {
        //TODO 1,获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);//生产环境设置为Kafka主题的分区数量
        //1.1开启checkPoint,5分钟一次
        environment.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().setCheckpointTimeout(10*60000L);
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        //1.2设置状态后端
        environment.setStateBackend(new HashMapStateBackend());
        environment.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/211126/ck");
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 2,读取Kafka topic_db主题数据创建主流
        String topic = "topic_db";
        String groupId = "Dim_App_211126";
        DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3，过滤非JSON数据以及保留新增，变化以及初始化数据为json
//        kafkaDS.flatMap()
        SingleOutputStreamOperator<JSONObject> filterJsonObjectDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    //将数据转为json
                    JSONObject jsonObject = JSON.parseObject(s);
                    String type = jsonObject.getString("type");
                    System.out.println(type);
                    //保留新增,变化，以及初始化数据
                    if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {
                        collector.collect(jsonObject);
                    }

                } catch (Exception e) {
                    System.out.println("发现脏数据" + s);
                }

            }
        });
        Properties prop = new Properties();
        prop.setProperty("useSSL","false");
        //TODO 4，使用flinkCDC 读取mysql配置信息表创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.10.102")
                .port(3306)
                .username("root")
                .password("Estim@b509")
                .databaseList("gmail-config")
                .tableList("gmail-config.table_process")
//                .databaseList("gmall")
//                .tableList("gmall.base_region")
//                .hostname("192.168.1.133")
//                .port(3306)
//                .username("root")
//                .password("Estim@b509")
//                .databaseList("cdc_test")
//                .tableList("cdc_test.user_info")
//                .serverTimeZone("UTC")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .jdbcProperties(prop)
                .build();

//
        DataStreamSource<String> streamSource = environment.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQLSource");
        streamSource.print();
        environment.execute();



        //TODO 5，将配置流处理为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state",String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = streamSource.broadcast(mapStateDescriptor);
        //TODO 6，连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterJsonObjectDS.connect(broadcastStream);

        //TODO 7，处理连接流，根据配置信息处理主流数据
        SingleOutputStreamOperator<JSONObject> dimDs = connectedStream.process(new TableProcessFunction(mapStateDescriptor));
        //TODO 8，将数据写出到Phoenix
        dimDs.print(">>>>>>>>");
        //TODO 9 启动任务
        environment.execute("dimApp");
    }
}
