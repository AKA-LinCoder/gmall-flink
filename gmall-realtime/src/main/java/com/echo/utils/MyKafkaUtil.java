package com.echo.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.shuffle.FlinkKafkaShuffle;
import org.apache.flink.streaming.connectors.kafka.shuffle.FlinkKafkaShuffleConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class MyKafkaUtil {

    private static final String Kafka_server = "hadoop102:9092";


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
}
