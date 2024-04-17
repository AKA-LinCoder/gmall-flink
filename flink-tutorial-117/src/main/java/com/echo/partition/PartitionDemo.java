package com.echo.partition;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        DataStreamSource<String> streamSource = environment.socketTextStream("hadoop102", 7777);
        ///随机分区 random.nextInt（下游算子并行度）
        streamSource.shuffle().print();
        //rebalance轮询 nextChannel = (nextChannel+1)% 下游算子并行度
        //如果是数据倾斜的场景，source读进来只来，调用rebalance，就可以解决数据源的数据倾斜
        streamSource.rebalance().print();
        //rescale缩放 实现轮询，局部组队，比rebalance更高效
        streamSource.rescale().print();
        //广播，发送给下游所有的子任务
        streamSource.broadcast().print();
        //global 全局，全部发送给第一个子任务
        streamSource.global().print();
        //keyby 相同key的发往同一个分区
        //flink提供了7种分区器+一个自定义
        DataStream dataStream = streamSource.partitionCustom(new MyPartition(), new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s;
            }
        });
        dataStream.print();
        environment.execute();
    }
}
