package com.echo.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.echo.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//CEP 部分听不懂

//数据流 web/app 发送请求 ->nginx -> 日志服务器(.log) -> flume -> Kafka(ods) -> flinkApp -> Kafka(DWD) ->FlinkApp -> Kafka(dwd)
//程序 Mock(lg.sh) -> flume -> kafka -> baseLogApp -> Kafka(Zk) -> DwdTrafficUserJumpDetail -> Kafka(Zk)

public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //TODO 读取Kafka页面日志主题数据创建流
        String topic = "dwd_traffic_page_log_1";
        String groupID = "traffic_user_jump_detail";
        DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupID));
        //TODO 将每行数据转化为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(line -> JSON.parseObject(line));
        //TODO 提取事件时间&&按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }))
                .keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        //TODO 定义cep的模式序列

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));

        //另一种写法
        //Times默认宽松近邻
//        Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject jsonObject) throws Exception {
//                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
//            }
//        })
//                //Times默认宽松近邻 followedBy
//                .times(2)
//                //严格近邻next
//                .consecutive().within(Time.seconds(10));
        //TODO 将模式序列作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);
        //TODO 提取事件（匹配上的事件以及超时事件）
        OutputTag<String> timeOutTag = new OutputTag<>("timeOut") {
        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, String>() {
            @Override
            public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        });
        //selectDS 正常输出
        //timeOutDS 侧输出流
        SideOutputDataStream<String> timeOutDS = selectDS.getSideOutput(timeOutTag);
        //TODO 合并两种事件
        DataStream<String> unionDS = selectDS.union(timeOutDS);
        //TODO 将数据写到Kafka
        selectDS.print("SELECT >>>>");
        timeOutDS.print("TIMEOUT >>>>");
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));
        //TODO 启动
        environment.execute("DwdTrafficUserJumpDetail");

        //WaterMark可以结合开窗处理乱序数据，表示小于Watermark数据已经到齐
        //维度退化


        //FlinkSQL
        //inner join 左表：onCreateAndWrite 右表 onCreateAndWrite
        //left join 左表：onReadAndWrite 右表：onCreateAndWrite
        //right join 左表：onCreateAndWrite 右表：onReadAndWrite

    }
}
