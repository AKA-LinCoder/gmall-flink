package com.echo.checkpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;

public class KafkaEOSDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        environment.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/chk");
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //TODO 读取Kafka
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setGroupId("kafkaEOS")
                .setTopics("topic_1")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                //作为下游的消费者，要设置事物的隔离级别
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
                .setStartingOffsets(OffsetsInitializer.latest()).build();
        DataStreamSource<String> dataStreamSource = environment.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)), "kafkaSource");
        //TODO 写出Kafka
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setRecordSerializer( KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("topic_2")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                //写到Kafka的一致性级别，精准一次（必须设置事物的前缀,事物超时时间，开启checkpoint），至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //如果精准一次，必须设置事务超时时间，大于checkPoint间隔，小于Max15分组
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10*60*1000+"")
                .setTransactionalIdPrefix("Isaac")
                .build();
        dataStreamSource.sinkTo(kafkaSink);

        environment.execute();
    }
}
