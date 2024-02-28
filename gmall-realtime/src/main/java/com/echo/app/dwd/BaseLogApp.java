package com.echo.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.echo.utils.DateFormatUtil;
import com.echo.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);//生产环境设置为Kafka主题的分区数量
        //TODO 2 消费Kafka topic_log 主题的数据创建流
        String topic = "topic_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //TODO 3 过滤掉非json格式数据&将每行数据转换为json对象
        OutputTag<String> dirtyTag = new OutputTag<>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    //没有发生异常就输出到主流
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    //异常数据写入到侧输出流
                    context.output(dirtyTag, s);
                }

            }
        });
        //获取侧输出流脏数据
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("存在脏数据>>>>>>>>");

        //TODO 4 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        //TODO 5 使用状态编程做新老访客的标签校验 1代表新用户 0 代表老客户
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            //状态编程
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                //获取is_new标记 & ts
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                Long ts = jsonObject.getLong("ts");
                //时间戳->年月日
                String currentDate = DateFormatUtil.toDate(ts);
                //获取状态中的日期
                String lastVisitDate = lastVisitState.value();
                //判断isNew是否为1
                if ("1".equals(isNew)) {
                    if (lastVisitDate == null) {
                        lastVisitState.update(currentDate);
                    } else if (!lastVisitDate.equals(currentDate)) {
                        //状态中记录着一个老日期，代表在此次登录前已经注册使用过
                        jsonObject.getJSONObject("common").put("id_new", "0");
                    }
                } else if (lastVisitDate == null) {
                    //代表isNew为0，并且 lastVisitDate为null
                    //将首次访问时间设置为昨天
                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));

                }

                return jsonObject;
            }
        });
        //TODO 6 使用侧输出流进行分流处理,页面日志放到主流，启动 曝光 动作 错误放到侧输出流
        OutputTag<String> startTag = new OutputTag<>("start");
        OutputTag<String> displayTag = new OutputTag<>("display");
        OutputTag<String> actionTag = new OutputTag<>("action");
        OutputTag<String> errorTag = new OutputTag<>("error");
        //侧输出流只能用process
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {

                //尝试获取错误信息
                String err = jsonObject.getString("err");
                if (err != null) {
                    //将数据写到侧输出流
                    context.output(errorTag, jsonObject.toJSONString());
                }
                //移除错误信息
                jsonObject.remove("err");

                //尝试获取启动信息
                //启动日志和页面日志属于互斥关系
                String start = jsonObject.getString("start");
                if (start != null) {
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    //获取公共信息&页面ID&时间戳
                    String common = jsonObject.getString("common");
                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                    Long ts = jsonObject.getLong("ts");
                    //尝试获取曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //遍历曝光数据
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            //将数据写到侧输出流
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                    //尝试获取动作数据
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        //遍历曝光数据
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page_id", pageId);
                            //将数据写到侧输出流
                            context.output(actionTag, action.toJSONString());
                        }
                    }

                    //因为启动页面不会有曝光和动作数据
                    //移除曝光和动作数据&写到页面日志主流
                    jsonObject.remove("display");
                    jsonObject.remove("action");
                    collector.collect(jsonObject.toJSONString());

                }

            }
        });
        //TODO 7 提取各个侧输出流数据
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        SideOutputDataStream<String> errorDS = pageDS.getSideOutput(errorTag);
        //TODO 8 将数据打印并写入对应的主题
        startDS.print("Start:>>>>>>>");
        displayDS.print("Display:>>>>>>>");
        actionDS.print("Action:>>>>>>>");
        errorDS.print("Error:>>>>>>>");

        // 定义不同日志输出到 Kafka 的主题名称
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));
        //生产者
        //bin/kafka-console-producer.sh --broker-list hadoop102:9092 --topic topic_log
        //消费者 如果没有主题 会有警告但是不用管 生产者会创建主题，
        //bin/kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic dwd_traffic_page_log

        //TODO 9 启动任务
        environment.execute("BaseLogApp");
    }
}
