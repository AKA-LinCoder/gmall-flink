package com.echo.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.echo.bean.TrafficHomeDetailPageViewBean;
import com.echo.utils.DateFormatUtil;
import com.echo.utils.MyClickHouseUtil;
import com.echo.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//页面浏览数据获取
public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //TODO  读取Kafka页面日志主题数据创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";
        DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //TODO 将每行数据转换为json对象并过滤
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {

                JSONObject jsonObject = JSON.parseObject(s);
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                    collector.collect(jsonObject);
                }
            }
        });
        //TODO 提取事件时间生成watermark
        SingleOutputStreamOperator<JSONObject> jsonWithWaterMarkDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));
        //TODO 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonWithWaterMarkDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        //TODo 使用状态编程过滤出首页与商品详情页面的独立访客数据
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeLastState;
            private ValueState<String> detailLastState;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> homeStateDes = new ValueStateDescriptor<>("home-state", String.class);
                ValueStateDescriptor<String> detailStateDes = new ValueStateDescriptor<>("detail-state", String.class);

                //设置TTL
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                homeStateDes.enableTimeToLive(ttlConfig);
                detailStateDes.enableTimeToLive(ttlConfig);


                homeLastState = getRuntimeContext().getState(homeStateDes);
                detailLastState = getRuntimeContext().getState(detailStateDes);

            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                //获取状态数据以及当前数据中的日期
                String curDt = DateFormatUtil.toDate(jsonObject.getLong("ts"));
                String homeLastDt = homeLastState.value();
                String detailLastDt = detailLastState.value();

                //定义访问首页或者详情页的数据
                long homeCt = 0L;
                long detailCt = 0L;

                //如果状态为空或者状态时间与当前时间不同，则为需要的数据
                if (homeLastDt == null || !homeLastDt.equals(curDt)) {
                    homeCt = 1L;
                    homeLastState.update(curDt);
                }

                if (detailLastDt == null || !detailLastDt.equals(curDt)) {
                    detailCt = 1L;
                    detailLastState.update(curDt);
                }

                //满足任何一个数据不等于） ，则可以写出
                if (homeCt == 1L || detailCt == 1L) {
                    collector.collect(new TrafficHomeDetailPageViewBean("", "", homeCt, detailCt, jsonObject.getLong("ts")));
                }


            }
        });
        //TODO 开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDS = trafficHomeDetailDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))).reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean t1) throws Exception {
                trafficHomeDetailPageViewBean.setHomeUvCt(trafficHomeDetailPageViewBean.getHomeUvCt() + t1.getHomeUvCt());
                trafficHomeDetailPageViewBean.setGoodDetailUvCt(trafficHomeDetailPageViewBean.getGoodDetailUvCt() + t1.getGoodDetailUvCt());
                return trafficHomeDetailPageViewBean;
            }
        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                //获取数据
                TrafficHomeDetailPageViewBean next = iterable.iterator().next();
                //补充字段
                next.setTs(System.currentTimeMillis());
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                //输出
                collector.collect(next);
            }
        });
        //TODO 将数据写出
        resultDS.print(">>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));
        //TODO 启动
        environment.execute("DwsTrafficPageViewWindow");
    }
}
