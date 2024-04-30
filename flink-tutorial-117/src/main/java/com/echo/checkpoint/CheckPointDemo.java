package com.echo.checkpoint;

import com.echo.bean.WaterSensor;
import com.echo.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

/**
 * 检查点算法总结
 * 1 Barrier对齐：一个task收到 所有上游同一个编号的barrier之后，才会对自己的本地状态做备份
 * 精准一次：对齐过程中，barrier后面的数据阻塞等待
 * 至少一次：在对齐过程中，先到的barrier其后面的数据不阻塞，接着计算
 *
 * 2 非Barrier对齐：flink1.11之后才有，一个task收到第一个barrier时，就开始做备份，能保证精准一次
 *  先到的barrier，将本地状态备份，其后面的数据接着计算输出
 *  未到的barrier，其前面的数据接着计算输出，同时也保存到备份中
 *  最后一个barrier到达该task时，这个task的备份结束
 */
public class CheckPointDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);


        //最终检查点:1.15开始默认开启


        //TODO 设置权限，避免Hadoop报错
        //代码中用到hdfs,导入Hadoop依赖，需要指定hdfs的用户名
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 启用检查点 周期为1S，默认是barrier对齐
        environment.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        //TODO 指定检查点的存储位置
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/chk");
        //TODO 指定checkpoint超时时间,默认10分钟
        checkpointConfig.setCheckpointTimeout(5000L);
        //TODO 同时运行中的checkPoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        //TODO 最小等待间隔 上一轮checkpoint结束到下一轮checkpoint开始之间的间隔，设置了>0，并发就会变成1
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        //TODO 取消作业时，checkpoint的数据保存是否保留在外部系统
        //DELETE_ON_CANCELLATION 主动cancel会正常删除 ，异常终止并不会删除数据
        //RETAIN_ON_CANCELLATION 什么情况都会保存
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //TODO 容忍失败的次数 默认0
        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        //TODO 开启非对齐检查点
        //开启要求：checkpoints模式必须时精准一次， 最大并发必须设置为1

        checkpointConfig.enableUnalignedCheckpoints(true);
        //开启非对齐检查点才生效，默认0。表示一啊开始就之间用非对齐的检查点
        //如果大于0，一开始用对齐的检查点（barrier对齐），对齐的时间超过这个参数自动切换成非对齐检查点
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1 ));

        DataStreamSource<String> socketTextStream = environment.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapDs = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strings = s.split(" ");
                for (String string : strings) {
                    Tuple2<String, Integer> tuple2 = Tuple2.of(string, 1);
                    collector.collect(tuple2);
                }
            }
        });
        flatMapDs.keyBy(value->value.f0).sum(1).print();
        environment.execute("");

    }
}
