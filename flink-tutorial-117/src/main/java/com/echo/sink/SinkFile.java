package com.echo.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;

/**
 * 将数据写入文件
 * 每个目录中，都有并行度个数的文件在写入
 */
public class SinkFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果有N个并行度，最大值为a
        //例如 2，100，就会从0和50开始，每个并行度生成50个
        environment.setParallelism(2);

        //必须开启
        environment.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        /**
         * 数据生成器参数
         *  第一个：输入类型固定是long
         *  第二个：自动生成的数字序列最大值
         *  第三个：限速策略
         *  第四个：返回类型
         */
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long aLong) throws Exception {
                        return "number" + aLong;
                    }
                }, 10, RateLimiterStrategy.perSecond(1), Types.STRING
        );
        DataStreamSource<String> dataStreamSource = environment.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "");
        //TODO 输出到文件系统
        FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path("/Users/estim/Desktop/keys/flink_test"), new SimpleStringEncoder<>("UTF-8"))
                ///文件滚动策略
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withInactivityInterval(Duration.ofSeconds(10))
                        .withMaxPartSize(1024*1024).build())
                //文件按照目录分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd", ZoneId.systemDefault()))
                ///指定前缀后缀
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("hhhh")
                        .withPartSuffix(".log")
                        .build())
                .build();
        dataStreamSource.sinkTo(fileSink);


        environment.execute();
    }
}
