package com.echo.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.echo.bean.UserLoginBean;
import com.echo.utils.DateFormatUtil;
import com.echo.utils.MyClickHouseUtil;
import com.echo.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.checkerframework.checker.units.qual.A;

import java.time.Duration;

//数据流 web/app 发送请求 ->nginx -> 日志服务器(.log) -> flume -> Kafka(ods) -> flinkApp -> Kafka(DWD) -> flinkApp -> clickhouse(DWs)
//程序 Mock(lg.sh) -> flume -> kafka -> baseLogApp -> Kafka(Zk) -> DwsUserUserLoginWindow -> clickhouse(ZK)
public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //TODO 读取Kafka页面日志主题创建流
        String topic = "dwd_traffic_page_log_1";
        String groupId = "dws_user_user_login_window";
        DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //TODO 转换数据为json对象并过滤数据
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String uid = jsonObject.getJSONObject("common").getString("uid");
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if (uid != null && (lastPageId == null || lastPageId.equals("login"))) {
                    collector.collect(jsonObject);
                }
            }
        });
        //TODO 提取事件时间生成watermark
        SingleOutputStreamOperator<JSONObject> timestampsAndWatermarks = jsonObjectDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));

        //TODO 按照uid分组
        KeyedStream<JSONObject, String> keyedStream = timestampsAndWatermarks.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("uid"));
        //TODO 使用状态编程获取独立用户以及七日回流用户
        SingleOutputStreamOperator<UserLoginBean> userLoginBeanSingleOutputStreamOperator = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {
            private ValueState<String> lastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastLoginState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-login", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<UserLoginBean> collector) throws Exception {

                //获取状态日期以及当前数据日期
                String lastLoginDt = lastLoginState.value();
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);

                //定义当日独立用户&七日回流用户数
                long uv = 0L;
                long backUv = 0L;

                if (lastLoginDt == null) {
                    uv = 1L;
                    lastLoginState.update(curDt);
                } else if (!lastLoginDt.equals(curDt)) {
                    uv = 1L;
                    lastLoginState.update(curDt);
                    if ((DateFormatUtil.toTs(curDt) - (DateFormatUtil.toTs(lastLoginDt)) / (24 * 60 * 60 * 1000L)) >= 8) {

                        backUv = 1L;
                    }
                }

                if (uv != 0L) {
                    collector.collect(new UserLoginBean("", "", backUv,uv, ts));
                }
            }
        });
        //TODO 开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resutlDS = userLoginBeanSingleOutputStreamOperator.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).reduce(new ReduceFunction<UserLoginBean>() {
            @Override
            public UserLoginBean reduce(UserLoginBean userLoginBean, UserLoginBean t1) throws Exception {
                userLoginBean.setBackCt(userLoginBean.getBackCt() + t1.getBackCt());
                userLoginBean.setUuCt(userLoginBean.getUuCt() + t1.getUuCt());
                return userLoginBean;
            }
        }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                UserLoginBean next = iterable.iterator().next();
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setTs(System.currentTimeMillis());
                collector.collect(next);
            }
        });
        //TODO 写入click house
        resutlDS.print(">>>>>>>>");
        resutlDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_user_user_login_window values(?,?,?,?,?)"));
        //TODO 启动
        environment.execute("DwsUserUserLoginWindow");
    }
}
