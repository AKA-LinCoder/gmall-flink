package com.echo.state;

import com.echo.bean.WaterSensor;
import com.echo.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

public class KeyedReducingStateDemo {
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

                            ReducingState<Integer> reducingState;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                //TODO 2 在open方法中初始化
                                reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("", new ReduceFunction<Integer>() {
                                    @Override
                                    public Integer reduce(Integer integer, Integer t1) throws Exception {
                                        return integer+t1;
                                    }
                                },Types.INT));
                            }


                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                reducingState.add(waterSensor.getVc());
                                collector.collect(
                                        "传感器id"+waterSensor.getId()+",水位值综合"+reducingState.get()
                                );
                            }
                        }).print();


        environment.execute();
    }
}
