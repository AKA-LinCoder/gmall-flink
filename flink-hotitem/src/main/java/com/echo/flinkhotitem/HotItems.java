package com.echo.flinkhotitem;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;

import org.apache.flink.api.java.io.PojoCsvInputFormat;

import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.io.File;

import java.net.URL;


public class HotItems {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        PojoTypeInfo pojoType  = (PojoTypeInfo)TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);

        DataStream dataSource = environment.createInput(csvInput,pojoType);
        ///从flink 1.12.0开始默认的时间语义为event-time。所以不需要再调用
//        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //获取一个带有时间标记的数据流
        dataSource.assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner((event,timestamp) -> event.timestamp *1000));
        SingleOutputStreamOperator<UserBehavior> timedData = dataSource.assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                        // 原始数据单位秒，将其转成毫秒
                        return userBehavior.timestamp*1000;
                    }
                }));
        // 过滤出只有点击的数据
//        DataStream<UserBehavior> pvData = timedData
//                .filter(new FilterFunction<UserBehavior>() {
//                    @Override
//                    public boolean filter(UserBehavior userBehavior) throws Exception {
//                        // 过滤出只有点击的数据
//                        return userBehavior.behavior.equals("pv");
//                    }
//                });
//        //窗口统计点击量
        KeyedStream keyBy = timedData.keyBy(userBehavior -> userBehavior.itemId);
        keyBy.map(new MapFunction<UserBehavior, String>() {
            @Override
            public String map(UserBehavior value) throws Exception {
                // 在这里对数据进行自定义格式化处理，然后返回格式化后的字符串
                return "自定义格式: " + value.behavior;
            }
        }).print(">>>>");
//        ///窗口大小是一个小时，每隔五分钟滑动一次
//        AllWindowedStream windowedStream = keyBy.windowAll(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(5)));
//
//        SingleOutputStreamOperator<ItemViewCount> windowedData = windowedStream.aggregate(new CountAgg(),new WindowResultFunction());
//
//        windowedData.keyBy(itemViewCount -> itemViewCount.windowEnd).print();

        environment.execute("job");


    }

}


