package com.echo.state;

import com.echo.bean.WaterSensor;
import com.echo.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class KeyedListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> hadoop102DS = environment.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, ts) -> element.getTs() * 1000L));

        hadoop102DS.keyBy(WaterSensor::getId)
                        .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                            //TODO 1 定义状态
                            ListState<Integer> vcListState;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                //TODO 2 在open方法中初始化
                                vcListState = getRuntimeContext().getListState(new ListStateDescriptor<>("vcListState",Types.INT));
                            }


                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                               //来一条存一条
                                vcListState.add(waterSensor.getVc());
                                // 从list状态拿出来，排序，取前3 只留三个最大的
                                Iterable<Integer> integers = vcListState.get();
                                ArrayList<Integer> arrayList = new ArrayList<>();
                                for (Integer integer : integers) {
                                    arrayList.add(integer);
                                }
                                arrayList.sort(new Comparator<Integer>() {
                                    @Override
                                    public int compare(Integer o1, Integer o2) {
                                        return o2-o1;
                                    }
                                });
                                //只保留最大的3个（list中的个数一定是连续变大的，一超过3就立即清理即可）
                                if(arrayList.size()>3){
                                    //将最后一个清➗，第四个
                                    arrayList.remove(3);
                                }
                                //更新
                                collector.collect("最大的事"+arrayList.toString());
                                vcListState.update(arrayList);
                            }
                        }).print();


        environment.execute();
    }
}
