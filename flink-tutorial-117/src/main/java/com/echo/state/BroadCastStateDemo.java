package com.echo.state;

import com.echo.bean.WaterSensor;
import com.echo.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 水位超过指定的阈值发送警告，阈值可以动态修改
 */
public class BroadCastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        /**
         * 状态后端 负责管理本地状态
         * hashmap
         *    存在TM的JVM的堆内存，读写快，缺点：存不了太多(受限于taskmanager的内存)
         * rocksdb
         *    存在tm所在节点的rocksdb数据库，存到磁盘中， 写 序列号，读 反序列化 读写相对慢一些，可以存很大的状态
         *  配置方式：
         *      1。配置文件 flink-conf.yaml
         *      2. 代码中指定
         *      3。 提交参数指定
         */
        //设置状态后端
        environment.setStateBackend(new HashMapStateBackend());
//        environment.
        //数据流
        SingleOutputStreamOperator<WaterSensor> hadoop102 = environment.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());

        //配置流(用来广播配置)
        DataStreamSource<String> configDS = environment.socketTextStream("hadoop102", 8888);
        //TODO 将配置流广播
        MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<String, Integer>("state", Types.STRING,Types.INT);
        BroadcastStream<String> broadcastDS = configDS.broadcast(mapStateDescriptor);
        //TODO 将数据流和广播后的配置流合并
        BroadcastConnectedStream<WaterSensor, String> connect = hadoop102.connect(broadcastDS);
        //TODO 调用process
        connect.process(new BroadcastProcessFunction<WaterSensor, String, String>() {
            @Override
            public void processElement(WaterSensor waterSensor, BroadcastProcessFunction<WaterSensor, String, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                //TODO 5 处理广播流 通过上下文获取广播状态，取出里面的值,数据流只能读取广播状态，不能修改
                ReadOnlyBroadcastState<String, Integer> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                //判断广播状态是否有数据，因为刚启动时，可能时数据流的第一条数据先来
                Integer threshold = broadcastState.get("threshold") == null?0:broadcastState.get("threshold");
                if(waterSensor.getVc()>threshold){
                    collector.collect(waterSensor+"超出阈值"+threshold);
                }
            }

            @Override
            public void processBroadcastElement(String s, BroadcastProcessFunction<WaterSensor, String, String>.Context context, Collector<String> collector) throws Exception {
                //TODO 4 处理配置流
                BroadcastState<String, Integer> broadcastState = context.getBroadcastState(mapStateDescriptor);
                broadcastState.put("threshold",Integer.valueOf(s));

            }
        }).print();
        environment.execute("");
    }
}
