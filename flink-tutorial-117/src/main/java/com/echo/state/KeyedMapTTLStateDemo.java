package com.echo.state;

import com.echo.bean.WaterSensor;
import com.echo.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

public class KeyedMapTTLStateDemo {
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
                            MapState<Integer,Integer> mapState;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(5)) //过期时间5S
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 写入和创建时更新过期时间
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //不返回过期的状态值
                                        .build();
                                //启用ttl
                                MapStateDescriptor<Integer, Integer> mapState1 = new MapStateDescriptor<>("mapState", Types.INT, Types.INT);
                                mapState1.enableTimeToLive(ttlConfig);
                                //TODO 2 在open方法中初始
                                mapState = getRuntimeContext().getMapState(mapState1);
                            }


                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                //判断是否存在对应vc的key
                                Integer vc = waterSensor.getVc();
                                if(mapState.contains(vc)){
                                    Integer count = mapState.get(vc);
                                    mapState.put(vc,count++);
                                }else {
                                    mapState.put(vc,1);
                                }

                                StringBuffer stringBuffer = new StringBuffer();
                                stringBuffer.append("传感器id"+waterSensor.getId());
                                for (Map.Entry<Integer, Integer> entry : mapState.entries()) {
                                    stringBuffer.append(entry.toString()+"\n");
                                }

                                stringBuffer.append("++++++++++++++++++");
                                collector.collect(stringBuffer.toString());
                            }
                        }).print();


        environment.execute();
    }
}
