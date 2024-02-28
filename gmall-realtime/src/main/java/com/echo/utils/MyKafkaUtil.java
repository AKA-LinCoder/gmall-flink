package com.echo.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Properties;

public class MyKafkaUtil {

    private static final String Kafka_server = "hadoop102:9092";


    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer1(String topic,String groupId){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,Kafka_server);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        ///SimpleStringSchema Flink 提供的用于将 Kafka 消息序列化为字符串的简单实现
        //不用这个的原因是为了处理空数据
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
    }


    public static KafkaSource<String> getFlinkKafkaSource(String topic,String groupId){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,Kafka_server);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Kafka_server)
                .setTopics(topic)
                .setGroupId(groupId)
                .setDeserializer(KafkaRecordDeserializationSchema.of(new MyKafkaDeserialization()) )
                .build();

        return kafkaSource;
    }


    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic,String groupId){

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,Kafka_server);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return new FlinkKafkaConsumer<String>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                System.out.println(consumerRecord.value());
                if(consumerRecord==null || consumerRecord.value() == null){
                    return null;
                }else {
                    return new String(consumerRecord.value());
                }
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        }, properties);

    }

    public static   FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic){
        return new FlinkKafkaProducer<String>(Kafka_server,topic,new SimpleStringSchema());
    }

    public static   FlinkKafkaProducer<String> getFlinkKafkaProducer2(String topic,String defaultTopic){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,Kafka_server);
        return new FlinkKafkaProducer<String>(defaultTopic, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
                if(s == null){
                    return new ProducerRecord<>(topic,"".getBytes());
                }
                return new ProducerRecord<>(topic,s.getBytes());
            }
        },properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

}

