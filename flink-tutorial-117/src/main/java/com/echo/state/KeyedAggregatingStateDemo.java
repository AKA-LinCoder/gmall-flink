package com.echo.state;

import com.echo.bean.WaterSensor;
import com.echo.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

/**
 * 计算每种传感器的平均水位
 */
public class KeyedAggregatingStateDemo {
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

                            AggregatingState<Integer,Double> aggregatingState;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                //TODO 2 在open方法中初始化
                                //Tuple2<Integer,Integer> 累加器的类型
                                aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<>("", new AggregateFunction<Integer, Tuple2<Integer,Integer>, Double>() {
                                    @Override
                                    public Tuple2<Integer, Integer> createAccumulator() {
                                        return Tuple2.of(0,0);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> add(Integer integer, Tuple2<Integer, Integer> integerIntegerTuple2) {
                                        return Tuple2.of(integerIntegerTuple2.f0+integer,integerIntegerTuple2.f1+1);
                                    }

                                    @Override
                                    public Double getResult(Tuple2<Integer, Integer> integerIntegerTuple2) {
                                        return integerIntegerTuple2.f0 *1D/ integerIntegerTuple2.f1;
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {

                                        //只用用到会话窗口才会用到
                                        return Tuple2.of(integerIntegerTuple2.f0+acc1.f0,integerIntegerTuple2.f1+acc1.f1);
                                    }
                                },Types.TUPLE(Types.INT,Types.INT)));
                            }


                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                aggregatingState.add(waterSensor.getVc());
                                collector.collect("平均水位值"+aggregatingState.get());
                            }
                        }).print();


        environment.execute();
    }
}
