package com.echo.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.echo.utils.DateFormatUtil;
import com.echo.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//流量域独立访客明细

//数据流 web/app 发送请求 ->nginx -> 日志服务器(.log) -> flume -> Kafka(ods) -> flinkApp -> Kafka(DWD) ->FlinkApp -> Kafka(dwd)
//程序 Mock(lg.sh) -> flume -> kafka -> baseLogApp -> Kafka(Zk) -> DwdTrafficUniqueVisitorDetail -> Kafka(Zk)

public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        //TODO 1 获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);//生产环境设置为Kafka主题的分区数量
        //TODO 读取Kafka页面日志主题创建流
        String topic = "dwd_traffic_page_log";
        String groupID = "Unique_visitor_detail";
        DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupID));
        //TODO 过滤上一跳页面不为null的数据并将每行数据转换为json
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    //获取上一跳页面ID
                    String lastPageID = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageID == null) {
                        collector.collect(jsonObject);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
//                    throw new RuntimeException(e);
                }
            }
        });
        //TODO 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        //TODO 使用状态编程实现按照mid的去重
        //使用定时器只能使用process
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);
                //设置状态TTL
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                lastVisitState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                //获取状态数据&当前数据中的时间戳
                String lastVisitDate = lastVisitState.value();
                Long ts = jsonObject.getLong("ts");
                String currentDate = DateFormatUtil.toDate(ts);

                if (lastVisitDate == null || !lastVisitDate.equals(currentDate)) {
                    lastVisitState.update(currentDate);
                    return true;
                } else {
                    return false;
                }

            }
        });
        //TODO 将数据写到Kafka
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        filterDS.print(">>>>>");
        filterDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));

        //TODO mission start！
        environment.execute("DwdTrafficUniqueVisitorDetail");


        //ETL
        //TTL

        //跳出明细需求
        //跳出说明：一次会话中只访问过一个页面
        //没有会话ID：连续的两条数据如果间隔时间很短，那么认为是同一次会话的访问记录
        //思路一 会话窗口：统计窗口中的数据条数，如果为1，则输出，反之丢弃
        //思路二 状态编程： 遇到last_page为null,取出状态数据  解决不了乱序问题
            //状态=null  定时器+将自身写入状态
            //状态！=null 输出状态+将自身写入状态
        //思路三 CEP（状态编程+within开窗）

    }
}
