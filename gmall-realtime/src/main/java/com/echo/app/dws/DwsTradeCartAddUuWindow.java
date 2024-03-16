package com.echo.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.echo.bean.CartAddUuBean;
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

//Mock-> Mysql ->maxwell -> kafka(zk) ->DwdTradeCartAdd -> kafka(zk) -> DwsTradeCartAddUuWindow -> clickhouse

public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                collector.collect(jsonObject);
            }
        });
        //生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                String operateTime = jsonObject.getString("operate_time");
                if (operateTime != null) {
                    return DateFormatUtil.toTs(operateTime, true);
                } else {
                    return DateFormatUtil.toTs(jsonObject.getString("create_time"),true);
                }
            }
        }));
        //按照user_id分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(jsonObject -> jsonObject.getString("user_id"));
        //使用状态编程
        SingleOutputStreamOperator<CartAddUuBean> cartAddDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {
            private ValueState<String> lastCartAddState;


            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("last-cart", String.class);
                stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);
                lastCartAddState = getRuntimeContext().getState(stringValueStateDescriptor);
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<CartAddUuBean> collector) throws Exception {

                //获取状态数据以及当前数据的日期
                String lastDt = lastCartAddState.value();
                String operateTime = jsonObject.getString("operate_time");
                String curDt = null;
                if(operateTime!=null){
                   curDt = operateTime.split(" ")[0];
                }else{
                    String createTime = jsonObject.getString("create_time");
                    curDt = createTime.split(" ")[0];
                }
                if (lastDt == null || !lastDt.equals(curDt)) {

                    lastCartAddState.update(curDt);
                    collector.collect(new CartAddUuBean("", "", 1L, null));
                }
            }
        });
        //开窗聚合
        SingleOutputStreamOperator<CartAddUuBean> resultDS = cartAddDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))).reduce(new ReduceFunction<CartAddUuBean>() {
            @Override
            public CartAddUuBean reduce(CartAddUuBean cartAddUuBean, CartAddUuBean t1) throws Exception {
                cartAddUuBean.setCartAddUuCt(cartAddUuBean.getCartAddUuCt() + t1.getCartAddUuCt());
                return cartAddUuBean;
            }
        }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<CartAddUuBean> iterable, Collector<CartAddUuBean> collector) throws Exception {

                CartAddUuBean cartAddUuBean = iterable.iterator().next();
                cartAddUuBean.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                cartAddUuBean.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                collector.collect(cartAddUuBean);
            }
        });
        //写入clickhouse
        resultDS.print("1111111:");
        resultDS.<CartAddUuBean>addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));
        environment.execute("DwsTradeCartAddUuWindow");
    }
}
