package com.echo.split;

import com.echo.bean.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

/**
 * 测输出流
 */
public class SideOutPutDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        DataStreamSource<WaterSensor> dataStreamSource = environment.fromCollection(Arrays.asList(new WaterSensor("s1", 2L, 3),
                new WaterSensor("s1",1l,3),
                new WaterSensor("s2",2L,1),
                new WaterSensor("s3",2L,1),
                new WaterSensor("s1",4L,9)));
        OutputTag<WaterSensor> stringOutputTag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> stringOutputTag2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        /**
         * 使用测输出流，实现分流
         */
        SingleOutputStreamOperator<WaterSensor> process = dataStreamSource.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, WaterSensor>.Context context, Collector<WaterSensor> collector) throws Exception {

                String id = waterSensor.getId();
                if("s1".equals(id)){
                    //如果是S1，放到侧输出流S1中
                    //这个范型是要放到测输出流中的数据的类型

                    context.output(stringOutputTag,waterSensor);
                } else if ("s2".equals(id)) {

                    //如果是S2，放到侧输出流S2中
                    context.output(stringOutputTag2,waterSensor);
                }else{
                    //其他放到主流中
                    collector.collect(waterSensor);
                }
            }
        });
        //打印主流
        process.print("主流");
        //从主流中根据标签获取侧输出流
        process.getSideOutput(stringOutputTag).print("s1");
        process.getSideOutput(stringOutputTag2).print("s2");
        environment.execute("");

    }
}
