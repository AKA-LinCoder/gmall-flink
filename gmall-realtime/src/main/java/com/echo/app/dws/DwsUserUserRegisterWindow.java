package com.echo.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.echo.bean.UserRegisterBean;
import com.echo.utils.DateFormatUtil;
import com.echo.utils.MyClickHouseUtil;
import com.echo.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//程序：mock-》mysql=> maxwell -> kafka(zk) -> dwdUserRegister -> kafka(zk) -> DwsUserUserRegisterWindow -> clickhouse
public class DwsUserUserRegisterWindow {
    public static void main(String[] args) throws Exception {
        //TODO  获取环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //TODO 读取Kafka dwd层用户注册主题数据创建流
        String topic = "dwd_user_register";
        String groupId = "dws_user_user_register_window";
        DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //TODO 将每行数据转换为javaBean
        SingleOutputStreamOperator<UserRegisterBean> userRegisterBeanDS = kafkaDS.flatMap(new FlatMapFunction<String, UserRegisterBean>() {
            @Override
            public void flatMap(String s, Collector<UserRegisterBean> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String createTime = jsonObject.getString("create_time");
                collector.collect(new UserRegisterBean("", "", 1L, DateFormatUtil.toTs(createTime,true)));
                return null;
            }
        });
        //TODO 提取时间戳生成watermark
        SingleOutputStreamOperator<UserRegisterBean> timestampsAndWatermarks = userRegisterBeanDS.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
            @Override
            public long extractTimestamp(UserRegisterBean userRegisterBean, long l) {
                return userRegisterBean.getTs();
            }
        }));
        //TODO 开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> resultDS = timestampsAndWatermarks.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).reduce(new ReduceFunction<UserRegisterBean>() {
            @Override
            public UserRegisterBean reduce(UserRegisterBean userRegisterBean, UserRegisterBean t1) throws Exception {
                userRegisterBean.setRegisterCt(userRegisterBean.getRegisterCt() + t1.getRegisterCt());
                return userRegisterBean;
            }
        }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<UserRegisterBean> iterable, Collector<UserRegisterBean> collector) throws Exception {
                UserRegisterBean next = iterable.iterator().next();
                next.setTs(System.currentTimeMillis());
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                collector.collect(next);
            }
        });
        //TODO 写入clickhouse
        resultDS.print(">>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_user_user_register_window values(?,?,?,?)"));
        //TODO 启动任务
        environment.execute("DwsUserUserRegisterWindow");
    }
}
